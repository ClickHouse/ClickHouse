#include <Core/Mongo/Document.h>
#include <Core/Mongo/DocumentCollectionShape.h>
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

    auto json_representation = document.getRapidJSONRepresentation();

    /** Only the fields below shape the answer this handler builds. A field that is not implemented -
      * a `collation`, which changes which documents a filter matches, or a `batchSize`, which bounds
      * how many documents a batch carries while everything is answered in the first one - is refused
      * rather than dropped, so that a `find` never answers a different query than the one it was
      * sent. `singleBatch` asks for exactly what this handler does.
      */
    static const std::unordered_set<String> supported_fields{"filter", "projection", "sort", "limit", "skip", "singleBatch", "hint"};
    rejectUnsupportedCommandFields(json_representation, supported_fields, "find");

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

    auto sorting = serializeMember(json_representation, "sort");
    if (!sorting.empty())
        sorting = modifyFilter(sorting);

    /// The database is passed to the parser separately: a collection named in the query text
    /// as `db.<collection>` keeps the text independent of the database name, which may itself
    /// be `db`.
    auto mongo_dialect_query = fmt::format("db.{}.find({})", collection.collection, serialized_filter);
    /// Mongo reads `limit: 0` as no limit at all and a negative limit as its absolute
    /// value, the same way `count` does. The magnitude is taken in the unsigned domain,
    /// where negating the smallest `Int64` is well-defined.
    if (limit && *limit != 0)
        mongo_dialect_query += fmt::format(".limit({})", *limit < 0 ? -static_cast<UInt64>(*limit) : static_cast<UInt64>(*limit));
    if (skip && *skip != 0)
        mongo_dialect_query += fmt::format(".skip({})", *skip);
    if (!sorting.empty())
        mongo_dialect_query += fmt::format(".sort({})", sorting);

    const auto max_query_size = mongo_dialect_query.size();
    /// The reparse runs under the parser limits of the session, the same ones the Mongo dialect
    /// parses under, so that the wire endpoint accepts exactly what the dialect accepts.
    const auto limits = executor->getParserLimits();
    auto parser = Mongo::ParserMongoQuery(max_query_size, limits.max_parser_depth, limits.max_parser_backtracks);
    auto ast = Mongo::parseMongoQuery(
        parser,
        mongo_dialect_query.data(),
        mongo_dialect_query.data() + mongo_dialect_query.size(),
        "",
        max_query_size,
        limits.max_parser_depth,
        limits.max_parser_backtracks,
        collection.database);

    /// A collection of documents addresses its fields as the paths of the document column, and a
    /// read of every field of them answers with the documents as they are stored.
    const bool holds_documents = adaptQueryToCollectionShape(ast, collection, executor, /* reads_whole_documents = */ true);

    String sql_query;
    {
        WriteBufferFromString sql_buffer(sql_query);
        ast->format(sql_buffer, IAST::FormatSettings(true));
    }

    sql_query += " SETTINGS allow_suspicious_types_in_order_by = 1, allow_suspicious_types_in_group_by = 1";

    /// Mongo reads a collection that does not exist as empty rather than raising an error.
    /// The query is translated first, so that a malformed query is still an error.
    if (!objectExists(executor, "TABLE", collection.getQualifiedName()))
        return makeEmptyCursorReply(collection);

    return executeSelectIntoCursor(sql_query, collection, executor, holds_documents);
}

void registerFindHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<FindHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
