#include "Core/Mongo/Handlers/Update.h"
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include "Parsers/IdentifierQuotingStyle.h"

#include <bson/bson.h>
#include <fmt/core.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::MongoProtocol
{

std::vector<Document> UpdateHandler::handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    const auto & update_info_doc = sections[0].documents[0];
    const auto & filter_doc = sections[1].documents[0];

    String table_name;
    {
        const auto & json_representation = update_info_doc.getRapidJsonRepresentation();
        table_name = json_representation["update"].GetString();
    }
    String serialized_filter;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        const auto & json_representation = filter_doc.getRapidJsonRepresentation();

        json_representation["q"].Accept(writer);
        serialized_filter = buffer.GetString();
    }
    String serialized_update;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        const auto & json_representation = filter_doc.getRapidJsonRepresentation();

        json_representation["u"].Accept(writer);
        serialized_update = buffer.GetString();
    }

    auto mongo_dialect_query = fmt::format("db.{}.updateMany({}, {})", table_name, serialized_filter, serialized_update);

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser, mongo_dialect_query.data(), mongo_dialect_query.data() + mongo_dialect_query.size(), "", 10000, 10000, 10000);

    String sql_query;
    WriteBufferFromString sql_buffer(sql_query);
    auto settings = IAST::FormatSettings(true, false, IdentifierQuotingRule::WhenNecessary, IdentifierQuotingStyle::BackticksMySQL);
    ast->format(sql_buffer, settings);
    while (sql_query.back() == 0)
        sql_query.pop_back();

    sql_query = fmt::format("ALTER TABLE {} {};", table_name, sql_query);
    while (sql_query.back() == 0)
        sql_query.pop_back();
    executor->execute(sql_query);

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", 0);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc);
    return {doc};
}

void registerUpdateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<UpdateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
