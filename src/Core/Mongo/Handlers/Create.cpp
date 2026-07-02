#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Create.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

namespace DB::MongoProtocol
{

std::vector<Document> CreateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & request_doc = documents[0].documents[0];
    auto json = request_doc.getRapidJsonRepresentation();
    String table_name = json["drop"].GetString();

    {
        auto query = fmt::format("CREATE TABLE IF NOT EXISTS {} (json JSON) ENGINE = Memory SETTINGS enable_json_type = 1;", table_name);
        executor->execute(query);
    }

    bson_t * bson_doc = bson_new();
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc);
    return {doc};
}

void registerCreateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<CreateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
