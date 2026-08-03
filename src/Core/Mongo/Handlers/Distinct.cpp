#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Distinct.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
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

    {
        /// MongoDB returns the distinct values in ascending order, and a `GROUP BY` alone
        /// leaves the order arbitrary.
        rapidjson::Value direction(1);
        rapidjson::Value sort_key(rapidjson::kObjectType);
        sort_key.AddMember("_id", direction, allocator);

        rapidjson::Value sort(rapidjson::kObjectType);
        sort.AddMember("$sort", sort_key, allocator);
        pipeline.PushBack(sort, allocator);
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
    sql_query += " FORMAT JSON";

    auto output = executor->execute(sql_query);

    rapidjson::Document result_json;
    if (result_json.Parse(output.data()).HasParseError())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not parse the result of the query");

    auto data_it = result_json.FindMember("data");
    if (data_it == result_json.MemberEnd() || !data_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no rows");

    /// The reply is `{"values": [...], "ok": 1}`, and the values keep the types the rows carry, so
    /// it is built as JSON and converted to BSON once rather than value by value.
    rapidjson::Document reply;
    auto & reply_allocator = reply.GetAllocator();
    reply.SetObject();

    rapidjson::Value values(rapidjson::kArrayType);
    for (auto & row : data_it->value.GetArray())
    {
        auto value_it = row.FindMember("_id");
        if (value_it == row.MemberEnd())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The result of the query has no '_id'");
        rapidjson::Value value;
        value.CopyFrom(value_it->value, reply_allocator);
        values.PushBack(value, reply_allocator);
    }

    reply.AddMember("values", values, reply_allocator);
    reply.AddMember("ok", 1.0, reply_allocator);

    String serialized_reply;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        reply.Accept(writer);
        serialized_reply = buffer.GetString();
    }

    std::vector<Document> result;
    result.emplace_back(serialized_reply);
    return result;
}

void registerDistinctHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DistinctHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
