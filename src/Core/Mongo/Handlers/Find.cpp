#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Find.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <algorithm>

#include <bson/bson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
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

    auto json_representation = document.getRapidJSONRepresentation();
    rejectUnsupportedOptions(
        json_representation, "find", {"collation", "let", "min", "max", "returnKey", "showRecordId", "tailable", "awaitData"});
    /// `allowDiskUse: false` forbids a blocking sort to spill to disk, but ClickHouse spills a large
    /// sort by default (see `max_bytes_ratio_before_external_sort`), the same as for `aggregate`.
    if (!getBoolOption(json_representation, "allowDiskUse", "find").value_or(true))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The option 'allowDiskUse: false' of a 'find' command is not supported");

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

    auto limit = getWholeNumberOption(json_representation, "limit", "find");
    auto skip = getWholeNumberOption(json_representation, "skip", "find");
    if (skip && *skip < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'skip' of a 'find' command must not be negative");

    /// Mongo reads `limit: 0` as no limit at all and a negative limit as its absolute
    /// value, the same way `count` does. The magnitude is taken in the unsigned domain,
    /// where negating the smallest `Int64` is well-defined. Zero means no limit.
    UInt64 limit_magnitude = 0;
    if (limit)
        limit_magnitude = *limit < 0 ? -static_cast<UInt64>(*limit) : static_cast<UInt64>(*limit);

    /// The reply always holds the whole result in its first batch and closes the cursor (see
    /// `executeSelectIntoCursor`). Without `singleBatch`, `batchSize` only says how the result is
    /// split into batches, so a single batch is the same answer. With `singleBatch` the cursor is
    /// closed after the first batch, so `batchSize` bounds the number of documents returned, which
    /// is a limit.
    auto batch_size = getWholeNumberOption(json_representation, "batchSize", "find");
    if (batch_size && *batch_size < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'batchSize' of a 'find' command must not be negative");
    if (getBoolOption(json_representation, "singleBatch", "find").value_or(false) && batch_size)
    {
        if (*batch_size == 0)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "A 'batchSize' of 0 together with 'singleBatch' of a 'find' command is not supported");
        const auto batch_limit = static_cast<UInt64>(*batch_size);
        limit_magnitude = limit_magnitude == 0 ? batch_limit : std::min(limit_magnitude, batch_limit);
    }

    auto sorting = serializeMember(json_representation, "sort");
    if (!sorting.empty())
        sorting = modifyFilter(sorting);

    /// The database and the collection are passed to the parser separately, so the text names
    /// neither of them, see `MONGO_DIALECT_PLACEHOLDER_NAMESPACE`.
    auto mongo_dialect_query = fmt::format("{}.find({})", MONGO_DIALECT_PLACEHOLDER_NAMESPACE, serialized_filter);
    if (limit_magnitude != 0)
        mongo_dialect_query += fmt::format(".limit({})", limit_magnitude);
    if (skip && *skip != 0)
        mongo_dialect_query += fmt::format(".skip({})", *skip);
    if (!sorting.empty())
        mongo_dialect_query += fmt::format(".sort({})", sorting);

    auto ast = parseMongoDialectCommand(mongo_dialect_query, collection, *executor);

    String sql_query;
    {
        WriteBufferFromString sql_buffer(sql_query);
        ast->format(sql_buffer, IAST::FormatSettings(true));
    }

    sql_query += " FORMAT JSON";
    sql_query += " SETTINGS allow_suspicious_types_in_order_by = 1";

    /// Mongo reads a collection that does not exist as empty rather than raising an error, and
    /// the placeholder of `createCollection` holds no document either.
    /// The query is translated first, so that a malformed query is still an error.
    if (!collectionHasSchema(collection, executor))
        return makeEmptyCursorReply(collection);

    return executeSelectIntoCursor(sql_query, collection, executor, getMaxTimeSettings(json_representation, "find"));
}

void registerFindHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<FindHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
