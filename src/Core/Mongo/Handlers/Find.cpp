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

    auto json_representation = document.getRapidJSONRepresentation();

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
    auto serialized_skip = serializeMember(json_representation, "skip");

    auto sorting = serializeMember(json_representation, "sort");
    if (!sorting.empty())
        sorting = modifyFilter(sorting);

    /// The database is passed to the parser separately: a collection named in the query text
    /// as `db.<collection>` keeps the text independent of the database name, which may itself
    /// be `db`.
    auto mongo_dialect_query = fmt::format("db.{}.find({})", collection.collection, serialized_filter);
    if (!serialized_limit.empty())
    {
        /// Mongo reads `limit: 0` as no limit at all and a negative limit as its absolute
        /// value, the same way `count` does.
        int limit = std::stoi(serialized_limit);
        if (limit != 0)
            mongo_dialect_query += fmt::format(".limit({})", limit < 0 ? -limit : limit);
    }
    if (!serialized_skip.empty())
        mongo_dialect_query += fmt::format(".skip({})", std::stoi(serialized_skip));
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

    /// Mongo reads a collection that does not exist as empty rather than raising an error.
    /// The query is translated first, so that a malformed query is still an error.
    if (!objectExists(executor, "TABLE", collection.getQualifiedName()))
        return makeEmptyCursorReply(collection);

    return executeSelectIntoCursor(sql_query, collection, executor);
}

void registerFindHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<FindHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
