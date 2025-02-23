#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Count.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>
#include <pcg_random.hpp>
#include "Common/Exception.h"
#include <Common/randomSeed.h>

#include <bson/bson.h>
#include <fmt/core.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::MongoProtocol
{

std::vector<Document> CountHandler::handle(const std::vector<OpMessageSection> & docs, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = docs[0].documents[0];
    const auto & json_representation = document.getRapidJsonRepresentation();
    String table_name = json_representation["count"].GetString();

    auto sql_query = "SELECT COUNT(*) FROM " + table_name;
    while (sql_query.back() == 0)
        sql_query.pop_back();

    std::vector<Document> result;
    bson_t * bson_doc = bson_new();
    auto output = executor->execute(sql_query);

    BSON_APPEND_INT32(bson_doc, "n", std::stoi(output));
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc);
    return {doc};
}

void registerCountHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<CountHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
