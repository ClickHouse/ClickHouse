#pragma once

#include <memory>
#include <optional>

#include <rapidjson/document.h>

#include <Parsers/IParserBase.h>

namespace DB
{

namespace Mongo
{

/// Metadata for MongoDB queries. Contains the database and collection, the query type, the
/// limit and the order by data. It also owns the allocator of every `rapidjson::Value`
/// produced while parsing the query, so that no state is shared between concurrent parses.
class QueryMetadata
{
public:
    bool add_data_to_query = true;

    enum class QueryType : uint8_t
    {
        select = 0,
        insert_many = 1,
        insert_one = 2,
        delete_many = 3,
        update_many = 4,
        aggregate = 5,
    };

    static constexpr std::pair<const char *, QueryType> queryTypeKeyWords[] = {
        {"find", QueryType::select},
        {"insertMany", QueryType::insert_many},
        {"insertOne", QueryType::insert_one},
        {"deleteMany", QueryType::delete_many},
        {"updateMany", QueryType::update_many},
        {"aggregate", QueryType::aggregate},
    };

    static constexpr size_t queryTypeKeyWordsLength = sizeof(queryTypeKeyWords) / sizeof(queryTypeKeyWords[0]);

    QueryMetadata(
        std::string database_name_,
        std::string collection_name_,
        QueryType query_type_,
        std::optional<Int64> limit_,
        std::optional<Int64> offset_,
        std::optional<std::string> order_by_);

    /// Empty when the query does not name a database, which means the current one.
    const std::string & getDatabaseName() const { return database_name; }

    const std::string & getCollectionName() const { return collection_name; }

    QueryType getQueryType() const { return query_type; }

    std::optional<UInt64> getLimit() const { return limit; }

    std::optional<UInt64> getOffset() const { return offset; }

    std::optional<std::string> getOrderBy() const { return order_by; }

    /// The allocator of every value of the parsed query. It is owned by this object, which
    /// lives at least as long as the parsers referencing those values.
    rapidjson::Document::AllocatorType & getAllocator() { return allocator; }

private:
    std::string database_name;
    std::string collection_name;
    QueryType query_type;
    std::optional<UInt64> limit;
    std::optional<UInt64> offset;
    std::optional<std::string> order_by;
    rapidjson::Document::AllocatorType allocator;
};

/** Extracts the metadata of a query. `database` overrides the database named by the query
  * itself: the wire protocol takes it from `$db`, so it must not depend on the text at all,
  * while a query written by a user names the database itself.
  */
std::shared_ptr<QueryMetadata>
extractMetadataFromRequest(const char * begin, const char * end, const std::string & database = "");

}

}
