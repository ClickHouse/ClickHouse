#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Drop.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <fmt/format.h>

#include <Common/Exception.h>

namespace DB::MongoProtocol
{

namespace
{

void appendNamespaceNotFound(bson_t * bson_doc)
{
    BSON_APPEND_UTF8(bson_doc, "errmsg", "ns not found");
    BSON_APPEND_INT32(bson_doc, "code", 26);
    BSON_APPEND_UTF8(bson_doc, "codeName", "NamespaceNotFound");
    BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);
}

}

std::vector<Document> DropHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "drop");

    /// A field of the command that would change what the drop does or promises - a `maxTimeMS`
    /// bound - must be refused rather than acknowledged and ignored, the same way the read
    /// commands refuse theirs.
    rejectUnsupportedCommandFields(documents[0].documents[0].getRapidJSONRepresentation(), {}, "drop");

    /// A `drop` is a write: it is acknowledged when the table is gone, so a write concern that asks
    /// for more than that is refused here for the same reason it is refused for an `insert`.
    validateWriteConcern(documents[0].documents[0].getRapidJSONRepresentation(), "drop");
    String namespace_name = collection.database + "." + collection.collection;

    /// The reply is allocated only after the fallible work, so an error thrown by the probe or by
    /// the `DROP TABLE` - a missing grant, an unauthenticated session - does not leak it.
    bool dropped = false;
    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        try
        {
            executor->execute(fmt::format("DROP TABLE {}", collection.getQualifiedName()));
            dropped = true;
        }
        catch (const Exception & e)
        {
            /// Another session may drop the table - or its whole database - between the probe and
            /// the `DROP TABLE`, which then fails with `UNKNOWN_TABLE` or `UNKNOWN_DATABASE`. The
            /// namespace is gone either way, so answer what the probe would have answered.
            if (!failedOnMissingCollection(e, executor, collection))
                throw;
        }
    }

    bson_t * bson_doc = bson_new();
    if (dropped)
    {
        BSON_APPEND_INT32(bson_doc, "nIndexesWas", 1);
        BSON_APPEND_UTF8(bson_doc, "ns", namespace_name.c_str());
        BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);
    }
    else
    {
        /// This is what a Mongo server answers when the namespace does not exist. Clients
        /// treat it as success, so reporting it faithfully keeps `drop` idempotent for them.
        appendNamespaceNotFound(bson_doc);
    }

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerDropHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DropHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
