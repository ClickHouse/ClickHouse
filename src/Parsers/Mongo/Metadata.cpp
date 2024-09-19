#include "Metadata.h"

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
    const char * begin, const char * end, QueryType query_type_, std::optional<int> limit_, std::optional<std::string> order_by_)
    : collection_name(begin, end), query_type(query_type_), limit(limit_), order_by(order_by_)
{
}

std::shared_ptr<QueryMetadata> extractMetadataFromRequest(const char * begin, const char * end)
{
    auto [token_begin, token_end] = getMetadataSubstring(begin, end);

    const char * token_begin_collection_name = findKth<'.'>(token_begin, token_end, 1) + 1;
    const char * token_end_collection_name = findKth<'.'>(token_begin, token_end, 2);

    const char * token_begin_query_type = token_end_collection_name + 1;
    const char * token_end_query_type = token_end;

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid query");
    }

    MongoQueryKeyNameExtractor limit_extractor(".limit");
    auto limit = limit_extractor.extractInt(begin, end);

    MongoQueryKeyNameExtractor order_by_extractor(".sort");
    auto order_by = order_by_extractor.extractString(begin, end);

    return std::make_shared<QueryMetadata>(token_begin_collection_name, token_end_collection_name, *query_type, limit, order_by);
}

}

}
