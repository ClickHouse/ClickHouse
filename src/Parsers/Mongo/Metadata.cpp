#include <Parsers/Mongo/Metadata.h>

#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

QueryMetadata::QueryMetadata(
    std::string database_name_,
    std::string collection_name_,
    QueryType query_type_,
    std::optional<int> limit_,
    std::optional<std::string> order_by_)
    : database_name(std::move(database_name_))
    , collection_name(std::move(collection_name_))
    , query_type(query_type_)
    , limit(limit_)
    , order_by(order_by_)
{
}

std::shared_ptr<QueryMetadata> extractMetadataFromRequest(const char * begin, const char * end, const std::string & database)
{
    auto [token_begin, token_end] = getMetadataSubstring(begin, end);

    /// A query addresses a collection as `<database>.<collection>.<operation>(...)`. The
    /// literal `db` in place of the database name means the current database, which is what
    /// the `mongosh` shell writes. A database may itself be named `db`, so the wire protocol
    /// never relies on this and passes the database from `$db` explicitly instead.
    const char * token_end_database_name = findKth<'.'>(token_begin, token_end, 1);
    const char * token_begin_collection_name = token_end_database_name + 1;
    const char * token_end_collection_name = findKth<'.'>(token_begin, token_end, 2);

    const char * token_begin_query_type = token_end_collection_name + 1;
    const char * token_end_query_type = token_end;

    std::string database_name = database;
    if (database_name.empty())
    {
        database_name.assign(token_begin, token_end_database_name);
        if (database_name == "db")
            database_name.clear();
    }

    std::string collection_name(token_begin_collection_name, token_end_collection_name);
    if (collection_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: the collection name is empty");

    std::string key(token_begin_query_type, token_end_query_type);
    std::optional<QueryMetadata::QueryType> query_type;

    for (const auto & [key_query, query] : QueryMetadata::queryTypeKeyWords)
    {
        if (key_query == key)
        {
            query_type = query;
        }
    }

    if (!query_type)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query: unknown operation '{}'", key);
    }

    MongoQueryKeyNameExtractor limit_extractor(".limit");
    auto limit = limit_extractor.extractInt(begin, end);

    MongoQueryKeyNameExtractor order_by_extractor(".sort");
    auto order_by = order_by_extractor.extractString(begin, end);

    return std::make_shared<QueryMetadata>(std::move(database_name), std::move(collection_name), *query_type, limit, order_by);
}

}

}
