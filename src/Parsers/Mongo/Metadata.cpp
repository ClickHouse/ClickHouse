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
    std::optional<int> offset_,
    std::optional<std::string> order_by_)
    : database_name(std::move(database_name_))
    , collection_name(std::move(collection_name_))
    , query_type(query_type_)
    , limit(limit_)
    , offset(offset_)
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

    /** `.limit(...)`, `.skip(...)` and `.sort(...)` are suffixes of a `find`, so only the text that follows the
      * argument list of the `find` is searched for them, and the pattern is searched for as plain
      * text. Searching the whole query would read the argument as well, and a document is free to
      * hold the pattern in a value of its own: `db.users.find({"name": ".limit(1)"})` looks for a
      * name and asks for no limit, but scanning from the start would find one there and turn the
      * user's data into a `LIMIT`. Looking in any other kind of query would go wrong the same way -
      * an aggregation pipeline may hold a field path such as `$a.limit`.
      */
    std::optional<int> limit;
    std::optional<int> offset;
    std::optional<std::string> order_by;
    if (*query_type == QueryMetadata::QueryType::select)
    {
        const char * suffix_begin = getSettingsSubstring(begin, end).second + 1;

        /** The text handed here may reach to the end of everything the client sent, so the suffix
          * stops at the terminator of this query - otherwise a `find` without a limit would take
          * the one of a later query of the same multi query. The statements are told apart the
          * same way `tryParseMongoQuery` tells them apart: by a `;` outside a string literal, a
          * `;` inside the argument of a `.sort(...)` is part of the argument.
          */
        const char * suffix_end = findStatementEnd(suffix_begin, end);

        MongoQueryKeyNameExtractor limit_extractor(".limit");
        limit = limit_extractor.extractInt(suffix_begin, suffix_end);

        MongoQueryKeyNameExtractor offset_extractor(".skip");
        offset = offset_extractor.extractInt(suffix_begin, suffix_end);

        MongoQueryKeyNameExtractor order_by_extractor(".sort");
        order_by = order_by_extractor.extractString(suffix_begin, suffix_end);
    }

    return std::make_shared<QueryMetadata>(std::move(database_name), std::move(collection_name), *query_type, limit, offset, order_by);
}

}

}
