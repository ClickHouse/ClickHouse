#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Insert.h>
#include "Core/Mongo/Document.h"
#include "IO/WriteBufferFromString.h"

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <fmt/format.h>
#include <pcg_random.hpp>
#include "Common/Exception.h"
#include <Common/CurrentThread.h>
#include <Common/randomSeed.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <format>
#include <optional>
#include <vector>

namespace DB::MongoProtocol
{

namespace
{

std::optional<String> getSimpleTypeField(const rapidjson::Value & document)
{
    if (document.IsBool())
        return "bool";
    else if (document.IsInt())
        return "int";
    else if (document.IsInt64())
        return "long";
    else if (document.IsFloat())
        return "float";
    else if (document.IsDouble())
        return "double";
    else if (document.IsString())
        return "String";
    return std::nullopt;
}

std::vector<InsertHandler::DocumnetField> traverseDocumentTypes(const rapidjson::Value & document)
{
    std::vector<InsertHandler::DocumnetField> fields;
    for (auto it = document.MemberBegin(); it < document.MemberEnd(); ++it)
    {
        if (it->name == "_id")
            continue;
        auto maybe_type = getSimpleTypeField(it->value);
        if (maybe_type.has_value())
        {
            fields.push_back(InsertHandler::DocumnetField{
                .full_name = it->name.GetString(),
                .type = std::move(*maybe_type),
            });
        }
        else if (it->value.IsObject())
        {
            auto traversed_child = traverseDocumentTypes(it->value);
            for (auto && child : traversed_child)
            {
                fields.push_back(InsertHandler::DocumnetField{
                    .full_name = String(it->name.GetString()) + "." + std::move(child.full_name), .type = std::move(child.type)});
            }
        }
        else if (it->value.IsArray())
        {
            const auto & value_arr = it->value.GetArray();
            String element_type = "JSON";
            if (!value_arr.Empty())
            {
                if (auto maybe_element_type = getSimpleTypeField(value_arr[0]); maybe_element_type.has_value())
                    element_type = std::move(*maybe_element_type);
            }
            fields.push_back(InsertHandler::DocumnetField{
                .full_name = it->name.GetString(),
                .type = std::format("Array({})", element_type),
            });
        }
    }
    return fields;
}

std::unordered_map<String, String> traverseDocumentValues(const rapidjson::Value & document)
{
    std::unordered_map<String, String> values;
    for (auto it = document.MemberBegin(); it < document.MemberEnd(); ++it)
    {
        if (it->name == "_id")
            continue;
        String name = it->name.GetString();
        if (it->value.IsBool())
            values[name] = std::to_string(it->value.GetBool());
        else if (it->value.IsInt())
            values[name] = std::to_string(it->value.GetInt());
        else if (it->value.IsInt64())
            values[name] = std::to_string(it->value.GetInt64());
        else if (it->value.IsFloat())
            values[name] = std::to_string(it->value.GetFloat());
        else if (it->value.IsDouble())
            values[name] = std::to_string(it->value.GetDouble());
        else if (it->value.IsString())
            values[name] = fmt::format("'{}'", it->value.GetString());
        else if (it->value.IsObject())
        {
            auto values_child = traverseDocumentValues(it->value);
            for (const auto & [child_name, child_value] : values_child)
                values[name + "." + child_name] = child_value;
        }
        else if (it->value.IsArray())
        {
            String arr_value = "[";
            const auto & arr = it->value.GetArray();
            for (const auto * it_arr = arr.begin(); it_arr < arr.end(); ++it_arr)
            {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                it_arr->Accept(writer);

                std::string json_elem_representation = buffer.GetString();
                arr_value += json_elem_representation + ",";
            }
            arr_value.pop_back();
            arr_value = "]";
            values[name] = arr_value;
        }
    }
    return values;
}

}

void InsertHandler::createDatabase(const Document & doc, std::shared_ptr<QueryExecutor> executor)
{
    auto json = doc.getRapidJsonRepresentation();

    String dbname = json["$db"].GetString();
    {
        auto query = fmt::format("CREATE DATABASE IF NOT EXISTS {};", dbname);
        executor->execute(query);
    }

    {
        auto query = fmt::format("USE {}", dbname);
        executor->execute(query);
    }
}

String InsertHandler::createTable(const Document & doc, std::shared_ptr<QueryExecutor> executor, const std::vector<DocumnetField> & types)
{
    auto json = doc.getRapidJsonRepresentation();
    String table_name = json["insert"].GetString();

    String query = fmt::format("CREATE TABLE IF NOT EXISTS {}(", table_name);

    for (const auto & type : types)
    {
        query += fmt::format("{} {},", type.full_name, type.type);
    }
    query.pop_back();
    query += ") ENGINE = MergeTree ORDER BY " + types[0].full_name + ";";
    executor->execute(query);
    return table_name;
}

std::vector<Document> InsertHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto traversed_schema = traverseDocumentTypes(documents[1].documents[0].getRapidJsonRepresentation());
    auto table_name = createTable(documents[0].documents[0], executor, traversed_schema);
    std::unordered_map<String, size_t> indexes_traversed_schema;
    for (size_t i = 0; i < traversed_schema.size(); ++i)
        indexes_traversed_schema[traversed_schema[i].full_name] = i;

    size_t inserted_count = 0;
    for (size_t section_id = 1; section_id < documents.size(); ++section_id)
    {
        const auto & docs = documents[section_id].documents;
        for (const auto & doc : docs)
        {
            ++inserted_count;
            auto values = traverseDocumentValues(doc.getRapidJsonRepresentation());
            std::vector<String> ordered_values(values.size());
            for (const auto & [value_field_name, value_field_value] : values)
                ordered_values[indexes_traversed_schema[value_field_name]] = value_field_value;
            String insert_value;
            for (const auto & order_value : ordered_values)
                insert_value += order_value + ",";
            insert_value.pop_back();
            auto query = fmt::format("INSERT INTO {} VALUES ({})", table_name, insert_value);
            executor->execute(query);
        }
    }

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", inserted_count);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc);
    return {doc};
}

void registerInsertHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<InsertHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
