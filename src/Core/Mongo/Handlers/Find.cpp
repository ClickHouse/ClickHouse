#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Find.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

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

namespace
{

/// Serializes a member of a JSON object, or returns an empty string when it is absent.
String serializeMember(const rapidjson::Value & json, const char * name)
{
    auto it = json.FindMember(name);
    if (it == json.MemberEnd())
        return {};

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    it->value.Accept(writer);
    return buffer.GetString();
}

}

std::vector<Document> FindHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = documents[0].documents[0];
    auto collection = getCollectionRef(document, "find");

    auto json_representation = document.getRapidJsonRepresentation();

    /// `filter` is a document so it owns its allocator: it is serialized below and
    /// must stay valid (it must not reference a temporary document's allocator).
    rapidjson::Document filter;
    auto & filter_allocator = filter.GetAllocator();
    filter.SetObject();
    if (auto filter_it = json_representation.FindMember("filter"); filter_it != json_representation.MemberEnd())
    {
        if (!filter_it->value.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'filter' of a 'find' command must be a document");
        filter.CopyFrom(filter_it->value, filter_allocator);
    }
    if (auto projection_it = json_representation.FindMember("projection"); projection_it != json_representation.MemberEnd())
    {
        rapidjson::Value projection;
        projection.CopyFrom(projection_it->value, filter_allocator);
        filter.AddMember("$projection", projection, filter_allocator);
    }

    String serialized_filter;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        filter.Accept(writer);
        serialized_filter = buffer.GetString();
    }
    serialized_filter = modifyFilter(serialized_filter);

    auto serialized_limit = serializeMember(json_representation, "limit");

    auto sorting = serializeMember(json_representation, "sort");
    if (!sorting.empty())
        sorting = modifyFilter(sorting);

    /// The database is passed to the parser separately: a collection named in the query text
    /// as `db.<collection>` keeps the text independent of the database name, which may itself
    /// be `db`.
    auto mongo_dialect_query = fmt::format("db.{}.find({})", collection.collection, serialized_filter);
    if (!serialized_limit.empty())
        mongo_dialect_query += fmt::format(".limit({})", std::stoi(serialized_limit));
    if (!sorting.empty())
        mongo_dialect_query += fmt::format(".sort({})", sorting);

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser,
        mongo_dialect_query.data(),
        mongo_dialect_query.data() + mongo_dialect_query.size(),
        "",
        10000,
        10000,
        10000,
        collection.database);

    String sql_query;
    {
        WriteBufferFromString sql_buffer(sql_query);
        ast->format(sql_buffer, IAST::FormatSettings(true));
    }

    sql_query += " FORMAT JSON";
    sql_query += " SETTINGS allow_suspicious_types_in_order_by = 1";

    std::vector<Document> selected;
    {
        auto output = executor->execute(sql_query);

        rapidjson::Document result_json;
        if (result_json.Parse(output.data()).HasParseError())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not parse the result of the query");

        auto data_it = result_json.FindMember("data");
        if (data_it == result_json.MemberEnd() || !data_it->value.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no rows");

        for (const auto & json_data : data_it->value.GetArray())
        {
            rapidjson::StringBuffer json_buffer;
            rapidjson::Writer<rapidjson::StringBuffer> json_writer(json_buffer);
            json_data.Accept(json_writer);
            selected.emplace_back(String(json_buffer.GetString()));
        }
    }

    /// Build the `{"cursor": {"firstBatch": [...], "id": 0, "ns": "db.collection"}, "ok": 1}`
    /// reply. `bson_append_array_begin` turns `first_batch` into a writer into `cursor` and
    /// `bson_append_array_end` finishes it, so it must not be allocated separately.
    bson_t cursor;
    bson_init(&cursor);

    {
        static constexpr std::string_view key_identifier = "firstBatch";
        bson_t first_batch;
        bson_append_array_begin(&cursor, key_identifier.data(), static_cast<int>(key_identifier.size()), &first_batch);
        for (size_t i = 0; i < selected.size(); ++i)
        {
            auto key_str = std::to_string(i);
            bson_append_document(&first_batch, key_str.c_str(), static_cast<int>(key_str.size()), selected[i].getBson());
        }
        bson_append_array_end(&cursor, &first_batch);
    }
    BSON_APPEND_INT64(&cursor, "id", 0);
    String namespace_name = collection.database + "." + collection.collection;
    BSON_APPEND_UTF8(&cursor, "ns", namespace_name.c_str());

    bson_t * result_doc = bson_new();
    BSON_APPEND_DOCUMENT(result_doc, "cursor", &cursor);
    BSON_APPEND_DOUBLE(result_doc, "ok", 1.0);
    bson_destroy(&cursor);

    std::vector<Document> result;
    result.emplace_back(result_doc);
    return result;
}

void registerFindHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<FindHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
