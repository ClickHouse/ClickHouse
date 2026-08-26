#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Delete.h>
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

std::vector<Document> DeleteHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    if (documents.size() < 2 || documents[1].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'delete' command does not contain any filter");

    auto collection = getCollectionRef(documents[0].documents[0], "delete");

    /// The 'delete' command carries one or more delete specs, each with its own 'q' filter
    /// and 'limit'. Execute every spec; 'limit: 1' (deleteOne) cannot be expressed as a
    /// ClickHouse mutation over an unordered table, so it is rejected instead of being
    /// silently widened into deleteMany.
    /// Every spec is translated first, and only then executed: a malformed filter has to be an
    /// error whether the collection exists or not.
    /// A ClickHouse mutation is asynchronous and says nothing about the rows it will remove, so
    /// the documents a spec matches are counted with the very same filter, translated as a `find`,
    /// before the mutation is submitted. Without it the reply would claim that a successful
    /// `deleteMany` removed nothing.
    auto translate = [&](const String & mongo_dialect_query)
    {
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
        return sql_query;
    };

    std::vector<String> sql_queries;
    std::vector<String> select_queries;
    for (const auto & delete_spec : documents[1].documents)
    {
        String serialized_filter;
        {
            auto json_representation = delete_spec.getRapidJSONRepresentation();
            auto filter_it = json_representation.FindMember("q");
            if (filter_it == json_representation.MemberEnd())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'delete' command does not contain the 'q' filter");

            auto limit_it = json_representation.FindMember("limit");
            if (limit_it != json_representation.MemberEnd()
                && !(limit_it->value.IsNumber() && limit_it->value.GetDouble() == 0))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'delete' command supports only 'limit: 0' (deleteMany); deleting a limited number of documents is not supported");

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            filter_it->value.Accept(writer);
            serialized_filter = buffer.GetString();
        }
        serialized_filter = modifyFilter(serialized_filter);

        sql_queries.push_back(translate(fmt::format("db.{}.deleteMany({})", collection.collection, serialized_filter)));
        select_queries.push_back(translate(fmt::format("db.{}.find({})", collection.collection, serialized_filter)));
    }

    /// A delete from a collection that does not exist matches no document, which Mongo reports as
    /// a delete of zero documents rather than an error.
    Int64 deleted = 0;
    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        for (size_t i = 0; i < sql_queries.size(); ++i)
        {
            deleted += countMatchedRows(select_queries[i], executor);
            executor->execute(sql_queries[i]);
        }
    }

    bson_t * bson_doc = bson_new();

    if (deleted <= INT32_MAX)
        BSON_APPEND_INT32(bson_doc, "n", static_cast<int32_t>(deleted));
    else
        BSON_APPEND_INT64(bson_doc, "n", deleted);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerDeleteHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DeleteHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
