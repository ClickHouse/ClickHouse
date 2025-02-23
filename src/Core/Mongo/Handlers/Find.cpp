#include <Core/Mongo/Handlers/Find.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>
#include "Common/Exception.h"
#include "Core/Mongo/Document.h"
#include "Core/Mongo/Handler.h"
#include "Formats/FormatSettings.h"
#include "IO/WriteBuffer.h"

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

#include <bson/bson.h>
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<Document> FindHandler::handle(const std::vector<OpMessageSection> & docs, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = docs[0].documents[0];
    const auto & json_representation = document.getRapidJsonRepresentation();
    String table_name = json_representation["find"].GetString();

    bool has_projection = false;
    rapidjson::Value filter;
    if (json_representation.FindMember("projection") != json_representation.MemberEnd())
    {
        has_projection = true;
        rapidjson::Document new_filter;
        new_filter.CopyFrom(json_representation["filter"], new_filter.GetAllocator());

        rapidjson::Document new_projection;
        new_projection.CopyFrom(json_representation["projection"], new_projection.GetAllocator());

        new_filter.AddMember("$projection", new_projection.GetObject(), new_filter.GetAllocator());
        filter = new_filter.GetObject();
    }
    else
    {
        rapidjson::Document new_filter;
        new_filter.CopyFrom(json_representation["filter"], new_filter.GetAllocator());
        filter = new_filter.GetObject();
    }

    String serialized_filter;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        filter.Accept(writer);
        serialized_filter = buffer.GetString();
    }

    std::optional<int> limit = std::nullopt;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        if (json_representation.FindMember("limit") != json_representation.MemberEnd())
        {
            json_representation["limit"].Accept(writer);
            limit = std::stoi(buffer.GetString());
        }
    }

    String sorting;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        if (json_representation.FindMember("sort") != json_representation.MemberEnd())
        {
            json_representation["sort"].Accept(writer);
            sorting = buffer.GetString();
            sorting = modifyFilter(sorting);
        }
    }

    serialized_filter = modifyFilter(serialized_filter);

    auto mongo_dialect_query = fmt::format("db.{}.find({})", table_name, serialized_filter);
    if (limit)
        mongo_dialect_query += fmt::format(".limit({})", *limit);
    if (!sorting.empty())
        mongo_dialect_query += fmt::format(".sort({})", sorting);

    std::cerr << "mongo_dialect_query " << mongo_dialect_query << '\n';

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser, mongo_dialect_query.data(), mongo_dialect_query.data() + mongo_dialect_query.size(), "", 10000, 10000, 10000);

    String sql_query;
    WriteBufferFromString sql_buffer(sql_query);
    ast->format(sql_buffer, IAST::FormatSettings(true));

    sql_query = clearQuery(sql_query);
    if (has_projection)
        sql_query += " FORMAT JSON";
    sql_query += " SETTINGS allow_suspicious_types_in_order_by = 1";
    std::vector<Document> result;
    {
        auto output = executor->execute(sql_query);

        if (!has_projection)
        {
            auto data_items = splitByNewline(output);
            for (const auto & json : data_items)
            {
                rapidjson::Document doc;
                doc.Parse(json.data());

                auto it = doc.FindMember("_id");
                if (it != doc.MemberEnd())
                {
                    doc.EraseMember(it);
                }
                rapidjson::StringBuffer json_buffer;
                rapidjson::Writer<rapidjson::StringBuffer> json_writer(json_buffer);
                doc.Accept(json_writer);

                std::cerr << "return doc " << json_buffer.GetString() << '\n';
                result.push_back(Document(json_buffer.GetString()));
            }
        }
        else
        {
            std::cerr << output << '\n';
            rapidjson::Document doc;
            doc.Parse(output.data());

            auto all_jsons = doc["data"].GetArray();
            std::cerr << "all_jsons.Size() " << all_jsons.Size() << '\n';
            for (const auto & json_data : all_jsons)
            {
                rapidjson::StringBuffer json_buffer;
                rapidjson::Writer<rapidjson::StringBuffer> json_writer(json_buffer);
                json_data.Accept(json_writer);
                std::cerr << "result item " << json_buffer.GetString() << '\n';
                result.push_back(Document(json_buffer.GetString()));
            }
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
