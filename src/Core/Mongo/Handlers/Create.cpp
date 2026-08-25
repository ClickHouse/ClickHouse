#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Create.h>
#include <Parsers/Mongo/DocumentCollection.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <fmt/format.h>
#include <Common/quoteString.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
}

}

namespace DB::MongoProtocol
{

namespace
{

/// The reply Mongo sends for a namespace that already exists. A client tells this case apart from
/// any other failure by the code rather than by the message, and it is how it learns that somebody
/// else created the collection first.
std::vector<Document> namespaceExistsReply(const CollectionRef & collection)
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

}

std::vector<Document> CreateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    /// A collection option this handler does not implement - a `capped` size, a `validator`, a
    /// `collation` - and a field that would change what the command promises, such as a
    /// `maxTimeMS` bound, must be refused rather than acknowledged and ignored.
    rejectUnsupportedCommandFields(documents[0].documents[0].getRapidJSONRepresentation(), {}, "create");

    validateWriteConcern(documents[0].documents[0].getRapidJSONRepresentation(), "create");
    /// The collection to create is the value of the `create` field of the command itself.
    auto collection = getCollectionRef(documents[0].documents[0], "create");

    /// Creating a namespace that already exists is an error in Mongo, not a no-op: clients rely
    /// on the duplicate-namespace error to detect that somebody else created the collection first.
    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
        return namespaceExistsReply(collection);

    executor->execute(fmt::format("CREATE DATABASE IF NOT EXISTS {}", backQuoteIfNeed(collection.database)));

    /** A collection keeps whole documents in one `JSON` column, with the object id of each of them
      * in an `_id` column, which is the primary key: a Mongo collection has no schema, so there is
      * nothing to infer from the documents that arrive later either. It is the same shape a
      * collection created by the first `insert` gets. No `IF NOT EXISTS`: a collection created
      * concurrently between the probe above and this statement must surface as an error, not as a
      * false success - and it has to surface as the same duplicate-namespace error the probe
      * reports, rather than as the generic failure reply the exception would otherwise become.
      */
    try
    {
        executor->execute(fmt::format(
            "CREATE TABLE {} ({} String, {} JSON) ENGINE = MergeTree ORDER BY {} COMMENT {}",
            collection.getQualifiedName(),
            backQuoteIfNeed(String(Mongo::OBJECT_ID_COLUMN)),
            backQuoteIfNeed(String(Mongo::DOCUMENT_COLUMN)),
            backQuoteIfNeed(String(Mongo::OBJECT_ID_COLUMN)),
            quoteString(String(Mongo::DOCUMENT_COLLECTION_COMMENT))));
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::TABLE_ALREADY_EXISTS)
            throw;
        return namespaceExistsReply(collection);
    }

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
