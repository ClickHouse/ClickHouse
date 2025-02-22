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

std::vector<Document> CountHandler::handle(const std::vector<OpMessageSection> & docs, std::unique_ptr<Session> & session)
{
    const auto & document = docs[0].documents[0];
    const auto & json_representation = document.getRapidJsonRepresentation();
    String table_name = json_representation["count"].GetString();

    auto sql_query = "SELECT COUNT(*) FROM " + table_name;
    while (sql_query.back() == 0)
        sql_query.pop_back();

    std::cerr << "sql_query " << sql_query << ' ' << sql_query.size() << '\n';

    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    std::vector<Document> result;
    CurrentThread::QueryScope query_scope{query_context};
    ReadBufferFromString read_buf(sql_query + ";");

    WriteBufferFromOwnString out;
    executeQuery(read_buf, out, false, query_context, {});

    std::cerr << "result " << out.str() << '\n';

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", std::stoi(out.str()));
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc, false);
    return {doc};
}

void registerCountHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<CountHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
