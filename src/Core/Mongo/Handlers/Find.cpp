#include <Core/Mongo/Handlers/Find.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>
#include "Common/Exception.h"
#include "Core/Mongo/Document.h"
#include "Formats/FormatSettings.h"
#include "IO/WriteBuffer.h"

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

#include <bson/bson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<Document> FindHandler::handle(const std::vector<OpMessageSection> & docs, std::unique_ptr<Session> & session)
{
    const auto & document = docs[0].documents[0];
    const auto & json_representation = document.getRapidJsonRepresentation();
    String table_name = json_representation["find"].GetString();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    json_representation["filter"].Accept(writer);
    String serialized_filter = buffer.GetString();

    auto mongo_dialect_query = fmt::format("db.{}.find({})", table_name, serialized_filter);

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser, mongo_dialect_query.data(), mongo_dialect_query.data() + mongo_dialect_query.size(), "", 10000, 10000, 10000);

    String sql_query;
    WriteBufferFromString sql_buffer(sql_query);
    ast->format(sql_buffer, IAST::FormatSettings(true));

    while (sql_query.back() == 0)
        sql_query.pop_back();
    std::cerr << "sql_query " << sql_query << ' ' << sql_query.size() << '\n';

    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    auto query_context = session->makeQueryContext();
    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    std::vector<Document> result;
    {
        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(sql_query + ";");

        WriteBufferFromOwnString out;
        executeQuery(read_buf, out, false, query_context, {});

        auto data_items = splitByNewline(out.str());
        for (const auto & json : data_items)
        {
            std::cerr << "json " << json << '\n';
            rapidjson::Document doc;
            doc.Parse(json.data());

            auto it = doc.FindMember("_id");
            if (it == doc.MemberEnd())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Json from mongo does not contais _id column");
            }
            doc.EraseMember(it);

            rapidjson::StringBuffer json_buffer;
            rapidjson::Writer<rapidjson::StringBuffer> json_writer(json_buffer);
            doc.Accept(json_writer);

            std::cerr << "result json " << json_buffer.GetString() << '\n';
            result.push_back(Document(json_buffer.GetString()));
        }
    }

    bson_t * cursor_doc = bson_new();
    bson_t * selected_docs = bson_new();

    {
        String key_identifier = "firstBatch";
        bson_append_array_begin(selected_docs, key_identifier.c_str(), static_cast<Int32>(key_identifier.size()), cursor_doc);
        for (size_t i = 0; i < result.size(); ++i)
        {
            auto key_str = std::to_string(i);
            bson_append_document(cursor_doc, key_str.c_str(), static_cast<Int32>(key_str.size()), result[i].getBson());
        }
        bson_append_array_end(selected_docs, cursor_doc);
    }
    BSON_APPEND_INT64(selected_docs, "id", 0);
    String table_id = "db." + table_name;
    BSON_APPEND_UTF8(selected_docs, "ns", table_id.c_str());

    bson_t * result_doc = bson_new();
    BSON_APPEND_DOCUMENT(result_doc, "cursor", selected_docs);
    BSON_APPEND_DOUBLE(result_doc, "ok", 1.0);

    Document doc(result_doc, true);
    return {doc};
}

void registerFindHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<FindHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
