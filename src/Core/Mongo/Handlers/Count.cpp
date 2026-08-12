#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Count.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <bson/bson.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<Document> CountHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = documents[0].documents[0];
    auto collection = getCollectionRef(document, "count");

    auto json_representation = document.getRapidJSONRepresentation();

    /// `count` is the size of the result of a `find`, so its filter takes exactly the same
    /// path as the filter of a `find` - including the normalization of subdocument paths.
    String serialized_filter = "{}";
    if (auto query_it = json_representation.FindMember("query");
        query_it != json_representation.MemberEnd() && !query_it->value.IsNull())
    {
        if (!query_it->value.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'query' of a 'count' command must be a document");

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        query_it->value.Accept(writer);
        serialized_filter = modifyFilter(buffer.GetString());
    }

    /// The count options `limit` and `skip` bound the documents being counted. Mongo reads a
    /// negative `limit` as its absolute value, the same way `find` does.
    auto limit = getWholeNumberOption(json_representation, "limit", "count").value_or(0);
    auto skip = getWholeNumberOption(json_representation, "skip", "count").value_or(0);
    if (skip < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'skip' of a 'count' command must not be negative");

    auto mongo_dialect_query = fmt::format("db.{}.find({})", collection.collection, serialized_filter);
    if (skip != 0)
        mongo_dialect_query += fmt::format(".skip({})", skip);
    if (limit != 0)
        mongo_dialect_query += fmt::format(".limit({})", limit < 0 ? -static_cast<UInt64>(limit) : static_cast<UInt64>(limit));

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

    /// Mongo reads a collection that does not exist as empty rather than raising an error, so
    /// its `count` is 0. The query is translated first, so that a malformed query is still an
    /// error.
    Int64 count = 0;
    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        auto output = executor->execute(fmt::format("SELECT count() FROM ({}) FORMAT TSV", sql_query));

        /// A ClickHouse table is free to hold more rows than an `int32` can count.
        count = std::stoll(output);
    }

    bson_t * bson_doc = bson_new();

    if (count <= INT32_MAX)
        BSON_APPEND_INT32(bson_doc, "n", static_cast<int32_t>(count));
    else
        BSON_APPEND_INT64(bson_doc, "n", count);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerCountHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<CountHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
