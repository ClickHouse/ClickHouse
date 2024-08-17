#pragma once

#include <memory>
#include <optional>

#include <Parsers/IParserBase.h>

namespace DB
{

namespace Mongo
{

/// Metadata for MongoDB queries. Contains collection, query type, limi and order by data.
class QueryMetadata
{
public:
    bool add_data_to_query = true;

    enum class QueryType : uint8_t
    {
        select = 0,
        insert_many = 1,
        insert_one = 2,
    };

    static constexpr std::pair<const char *, QueryType> queryTypeKeyWords[] = {
        {"find", QueryType::select},
        {"insertMany", QueryType::insert_many},
        {"insertOne", QueryType::insert_one},
    };

    static constexpr size_t queryTypeKeyWordsLength = sizeof(queryTypeKeyWords) / sizeof(queryTypeKeyWords[0]);

    explicit QueryMetadata(
        const char * begin, const char * end, QueryType query_type_, std::optional<int> limit_, std::optional<std::string> order_by_);

    const std::string & getCollectionName() const { return collection_name; }

    QueryType getQueryType() const { return query_type; }

    std::optional<size_t> getLimit() const { return limit; }

    std::optional<std::string> getOrderBy() const { return order_by; }

private:
    std::string collection_name;
    QueryType query_type;
    std::optional<size_t> limit;
    std::optional<std::string> order_by;
};

std::shared_ptr<QueryMetadata> extractMetadataFromRequest(const char * begin, const char * end);

}

}
