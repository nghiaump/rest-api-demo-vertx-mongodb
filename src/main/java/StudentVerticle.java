import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;


import java.util.ArrayList;


public class StudentVerticle extends AbstractVerticle {
    private MongoClient client = null;
    private String collectionName = "students";

    private void getAllStudents(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json;charset=UTF-8");

        client.find(collectionName, new JsonObject(), res -> {
            if (res.succeeded()) {
                ArrayList<JsonObject> list = new ArrayList<>();
                for (JsonObject ob : res.result()) {
                    list.add(ob);
                }
                response.end(Json.encodePrettily(list));
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    private void getStudent(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json;charset=utf-8");
        String sid = routingContext.request().getParam("id");
        if (sid == null) {
            routingContext.response().setStatusCode(400).end();
        } else {
            int id = Integer.parseInt(sid);
            JsonObject query = new JsonObject().put("id", id);
            client.find(collectionName, query, res -> {
                if (res.succeeded()) {
                    if (res.result().isEmpty()) {
                        response.setStatusCode(404);
                        response.end("Student not found!");
                    } else {
                        ArrayList<JsonObject> list = new ArrayList<>();
                        for (JsonObject ob : res.result()) {
                            list.add(ob);
                        }
                        response.end(Json.encodePrettily(list));
                    }

                } else {
                    res.cause().printStackTrace();
                }
            });

        }
    }

    private void insertStudent(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json;charset=UTF8");
        try {
            JsonObject document = new JsonObject(routingContext.getBody());
            JsonObject query = new JsonObject().put("id", document.getValue("id"));
            client.find(collectionName, query, res -> {
                if (res.succeeded()) {
                    if (res.result().isEmpty()) {
                        // Target student is not exist before
                        client.insert(collectionName, document, res2 -> {
                            if (res2.succeeded()) {
                                String id = res2.result();
                                System.out.println("Inserted with id " + id);
                                //response.setStatusCode(200);
                                response.end("Inserted!");
                            } else {
                                res2.cause().printStackTrace();
                            }
                        });
                    } else {
                        // Insertion is not accepted
                        response.setStatusCode(406);
                        response.end("Student is existed!");
                    }
                } else {
                    res.cause().printStackTrace();
                }
            });

        } catch (Exception ex) {
            response.end(ex.getMessage());
        }
    }

    private void updateStudent(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json;charset=UTF8");

        try {
            JsonObject document = new JsonObject(routingContext.getBody());
            JsonObject target = new JsonObject().put("id", document.getValue("id"));
            JsonObject update = new JsonObject().put("$set", new JsonObject()
                    .put("name", document.getValue("name"))
                    .put("email", document.getValue("email")));
            client.updateCollection(collectionName, target, update, res -> {
                if (res.succeeded()) {
                    response.end("Updated! Student with id: " + Integer.toString((int) document.getValue("id")));
                } else {
                    res.cause().printStackTrace();
                }
            });
        } catch (Exception ex) {
            response.end(ex.getMessage());
        }

    }

    private void deleteStudent(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json;charset=UTF-8");

        String sid = routingContext.request().getParam("id");
        if (sid == null) {
            routingContext.response().setStatusCode(400).end();
        } else {
            int id = Integer.parseInt(sid);
            JsonObject query = new JsonObject().put("id", id);
            client.find(collectionName, query, res -> {
                if (res.succeeded()) {
                    if (res.result().isEmpty()) {
                        response.setStatusCode(404);
                        response.end("Student not found!");
                    } else {
                        for (JsonObject ob : res.result()) {
                            client.removeDocument(collectionName, ob);
                        }
                        response.end("Deleted!");
                    }
                } else {
                    res.cause().printStackTrace();
                }
            });

        }
    }

    @Override
    public void start(Promise startPromise) throws Exception {
        JsonObject config = new JsonObject().put("connection_string", "mongodb://localhost:27017/mydb");
        client = MongoClient.createShared(vertx, config);

        Router router = Router.router(vertx);
        router.get("/students").handler(this::getAllStudents);
        router.get("/students/:id").handler(this::getStudent);
        router.delete("/students/:id").handler(this::deleteStudent);
        router.route("/students*").handler(BodyHandler.create());
        router.post("/students").handler(this::insertStudent);
        router.put("/students").handler(this::updateStudent);
        vertx
                .createHttpServer()
                .requestHandler(router)
                .listen(8080);
    }
}
