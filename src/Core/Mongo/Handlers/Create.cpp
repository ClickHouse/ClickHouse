#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Create.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <fmt/format.h>
#include <Common/quoteString.h>

namespace DB::MongoProtocol
{

std::vector<Document> CreateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    /// The collection to create is the value of the `create` field of the command itself.
    auto collection = getCollectionRef(documents[0].documents[0], "create");

    executor->execute(fmt::format("CREATE DATABASE IF NOT EXISTS {}", backQuoteIfNeed(collection.database)));

    /// A collection created explicitly has no documents to infer a schema from, so it starts as
    /// a single `JSON` column. The first `insert` replaces that placeholder with a column per
    /// field of the inserted document, which is what a collection created implicitly by an
    /// `insert` gets right away.
    executor->execute(fmt::format(
        "CREATE TABLE IF NOT EXISTS {} (json JSON) ENGINE = MergeTree ORDER BY tuple()", collection.getQualifiedName()));

    bson_t * bson_doc = bson_new();
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerCreateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<CreateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
