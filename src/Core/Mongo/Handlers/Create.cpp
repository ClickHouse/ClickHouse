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

    /// Creating a namespace that already exists is an error in Mongo, not a no-op: clients rely
    /// on the duplicate-namespace error to detect that somebody else created the collection first.
    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        bson_t * error_doc = bson_new();

        String message = fmt::format("Collection already exists. NS: {}.{}", collection.database, collection.collection);
        BSON_APPEND_UTF8(error_doc, "errmsg", message.c_str());
        BSON_APPEND_INT32(error_doc, "code", 48);
        BSON_APPEND_UTF8(error_doc, "codeName", "NamespaceExists");
        BSON_APPEND_DOUBLE(error_doc, "ok", 0.0);

        std::vector<Document> result;
        result.emplace_back(error_doc);
        return result;
    }

    executor->execute(fmt::format("CREATE DATABASE IF NOT EXISTS {}", backQuoteIfNeed(collection.database)));

    /// A collection created explicitly has no documents to infer a schema from, so it starts as
    /// a single `JSON` column. The first `insert` replaces that placeholder with a column per
    /// field of the inserted document, which is what a collection created implicitly by an
    /// `insert` gets right away. No `IF NOT EXISTS`: a collection created concurrently between
    /// the probe above and this statement must surface as an error, not as a false success.
    executor->execute(fmt::format(
        "CREATE TABLE {} (json JSON) ENGINE = MergeTree ORDER BY tuple()", collection.getQualifiedName()));

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
