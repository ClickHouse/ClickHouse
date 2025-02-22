#include <Core/Mongo/Handlers/Delete.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>
#include "Core/Mongo/Document.h"

#include "IO/WriteBufferFromString.h"

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include "Common/Exception.h"
#include <Common/CurrentThread.h>
#include <Common/randomSeed.h>


#include <bson/bson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "Parsers/ParserQuery.h"
#include "Parsers/parseQuery.h"

namespace DB::MongoProtocol
{

namespace
{

std::vector<String> splitQuery(const String & query, char delim = ' ')
{
    std::vector<String> parts = {""};
    for (auto elem : query)
    {
        if (elem == delim && !parts.back().empty())
            parts.push_back("");

        if (elem != delim)
            parts.back().push_back(elem);
    }
    while (parts.back().empty())
        parts.pop_back();
    return parts;
}

/// Transform DELETE FROM table WHERE condition; to ALTER TABLE table DELETE WHERE condition;
String transformDeleteQueryToAlter(const String & query)
{
    auto parts = splitQuery(query);
    auto table_name = parts[2];
    String result = "ALTER TABLE " + table_name + " DELETE ";
    for (size_t i = 3; i < parts.size(); ++i)
        result += parts[i] + " ";
    result.pop_back();
    return result;
}
}

std::vector<Document> DeleteHandler::handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session)
{
    const auto & delete_info_doc = documents[0].documents[0];
    const auto & filter_doc = documents[1].documents[0];

    String table_name;
    {
        const auto & json_representation = delete_info_doc.getRapidJsonRepresentation();
        table_name = json_representation["delete"].GetString();
    }
    String serialized_filter;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        const auto & json_representation = filter_doc.getRapidJsonRepresentation();

        json_representation["q"].Accept(writer);
        serialized_filter = buffer.GetString();
    }
    serialized_filter = modifyFilter(serialized_filter);

    std::cerr << "table and filter " << table_name << ' ' << serialized_filter << '\n';

    auto mongo_dialect_query = fmt::format("db.{}.deleteMany({})", table_name, serialized_filter);

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser, mongo_dialect_query.data(), mongo_dialect_query.data() + mongo_dialect_query.size(), "", 10000, 10000, 10000);

    String sql_query;
    WriteBufferFromString sql_buffer(sql_query);
    ast->format(sql_buffer, IAST::FormatSettings(true));

    while (sql_query.back() == 0)
        sql_query.pop_back();
    std::cerr << "sql_query " << sql_query << ' ' << sql_query.size() << '\n';

    auto query_context = session->makeQueryContext();
    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    CurrentThread::QueryScope query_scope{query_context};
    sql_query = transformDeleteQueryToAlter(sql_query) + ";";
    //std::cerr << "final sql_query " << sql_query << '\n';
    //for (auto elem : sql_query)
    //    std::cerr << static_cast<Int32>(elem) << ' ' << elem << '\n';
    ReadBufferFromString read_buf(sql_query);
    WriteBufferFromOwnString out;
    executeQuery(read_buf, out, false, query_context, {});

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", 0);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc, true);
    return {doc};
}

void registerDeleteHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DeleteHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
