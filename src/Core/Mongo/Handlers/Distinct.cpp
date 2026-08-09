#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Distinct.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

std::vector<Document> DistinctHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = documents[0].documents[0];
    auto collection = getCollectionRef(document, "distinct");

    auto json_representation = document.getRapidJSONRepresentation();
    auto key_it = json_representation.FindMember("key");
    if (key_it == json_representation.MemberEnd() || !key_it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'distinct' command must name a field in 'key'");

    /// `distinct` is a `$group` on the field, so it goes through the pipeline translation rather
    /// than building a `SELECT DISTINCT` of its own.
    rapidjson::Document pipeline;
    auto & allocator = pipeline.GetAllocator();
    pipeline.SetArray();

    if (auto query_it = json_representation.FindMember("query");
        query_it != json_representation.MemberEnd() && query_it->value.IsObject() && query_it->value.MemberCount() > 0)
    {
        rapidjson::Value match(rapidjson::kObjectType);
        rapidjson::Value filter;
        filter.CopyFrom(query_it->value, allocator);
        match.AddMember("$match", filter, allocator);
        pipeline.PushBack(match, allocator);
    }

    {
        rapidjson::Value path(rapidjson::kStringType);
        path.SetString(("$" + String(key_it->value.GetString(), key_it->value.GetStringLength())).c_str(), allocator);

        rapidjson::Value key(rapidjson::kObjectType);
        key.AddMember("_id", path, allocator);

        rapidjson::Value group(rapidjson::kObjectType);
        group.AddMember("$group", key, allocator);
        pipeline.PushBack(group, allocator);
    }

    /// The `query` of a `distinct` becomes a `$match` stage, whose filter `serializePipeline`
    /// normalizes the same way the filter of a `find` is normalized.
    auto serialized_pipeline = serializePipeline(pipeline);

    auto mongo_dialect_query = fmt::format("db.{}.aggregate({})", collection.collection, serialized_pipeline);

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

    /// `distinct` on an array-valued field is element-wise in Mongo: a document whose field
    /// holds `["red", "blue"]` contributes the elements, not the array. `flatten([x])` reads a
    /// scalar as the one-element array and an array as its elements - the same normalization the
    /// filters use for `$eq` and `$in` - so one expression covers both; an array of arrays is
    /// flattened all the way down, the same known limitation those filters have. The grouped
    /// values of the pipeline are unwrapped, deduplicated again (two arrays are free to share
    /// elements) and returned in ascending order, which Mongo promises and a `GROUP BY` alone
    /// does not.
    sql_query = fmt::format(
        "SELECT DISTINCT arrayJoin(flatten([_id])) AS _id FROM ({}) ORDER BY _id ASC "
        "SETTINGS allow_suspicious_types_in_order_by = 1 FORMAT JSON",
        sql_query);

    /// Mongo reads a collection that does not exist as empty rather than raising an error, so
    /// its distinct values are `[]`. The query is translated first, so that a malformed query
    /// is still an error.
    if (!objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        bson_t * empty_reply = bson_new();

        bson_t values;
        static constexpr std::string_view key_identifier = "values";
        bson_append_array_begin(empty_reply, key_identifier.data(), static_cast<int>(key_identifier.size()), &values);
        bson_append_array_end(empty_reply, &values);
        BSON_APPEND_DOUBLE(empty_reply, "ok", 1.0);

        std::vector<Document> result;
        result.emplace_back(empty_reply);
        return result;
    }

    auto output = executor->execute(sql_query);

    rapidjson::Document result_json;
    if (result_json.Parse(output.data()).HasParseError())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not parse the result of the query");

    auto columns = extractResultColumns(result_json);
    if (columns.size() != 1 || columns[0].first != "_id")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no '_id'");

    auto data_it = result_json.FindMember("data");
    if (data_it == result_json.MemberEnd() || !data_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no rows");

    /// The reply is `{"values": [...], "ok": 1}`, and the values keep the types of the column
    /// (see `appendTypedValue`).
    bson_t * reply = bson_new();
    try
    {
        static constexpr std::string_view key_identifier = "values";
        bson_t values;
        bson_append_array_begin(reply, key_identifier.data(), static_cast<int>(key_identifier.size()), &values);
        size_t index = 0;
        for (const auto & row : data_it->value.GetArray())
        {
            /// `meta` promises the column, so every row has it.
            auto value_it = row.FindMember("_id");
            if (value_it != row.MemberEnd())
                appendTypedValue(&values, std::to_string(index++), value_it->value, columns[0].second);

            /// The `values` array grows in place inside `reply`, so its running size is exact;
            /// an oversized result is rejected before the whole reply is built.
            if (values.len > MAX_BSON_OBJECT_SIZE)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "The result is larger than the largest reply that can be sent ({} bytes). "
                    "Ask for less at a time, with a filter in 'query'",
                    MAX_BSON_OBJECT_SIZE);
        }
        bson_append_array_end(reply, &values);
        BSON_APPEND_DOUBLE(reply, "ok", 1.0);

        /// The bound holds for the reply document sent on the wire, envelope included.
        if (reply->len > MAX_BSON_OBJECT_SIZE)
            throw Exception(
                ErrorCodes::LIMIT_EXCEEDED,
                "The result is larger than the largest reply that can be sent ({} bytes). "
                "Ask for less at a time, with a filter in 'query'",
                MAX_BSON_OBJECT_SIZE);
    }
    catch (...)
    {
        bson_destroy(reply);
        throw;
    }

    std::vector<Document> result;
    result.emplace_back(reply);
    return result;
}

void registerDistinctHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DistinctHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
