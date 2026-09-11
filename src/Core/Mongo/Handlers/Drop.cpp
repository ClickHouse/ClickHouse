#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Drop.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <fmt/format.h>

namespace DB::MongoProtocol
{

std::vector<Document> DropHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "drop");
    String namespace_name = collection.database + "." + collection.collection;

    bson_t * bson_doc = bson_new();

    if (objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        executor->execute(fmt::format("DROP TABLE {}", collection.getQualifiedName()));

        BSON_APPEND_INT32(bson_doc, "nIndexesWas", 1);
        BSON_APPEND_UTF8(bson_doc, "ns", namespace_name.c_str());
        BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);
    }
    else
    {
        /// This is what a Mongo server answers when the namespace does not exist. Clients
        /// treat it as success, so reporting it faithfully keeps `drop` idempotent for them.
        BSON_APPEND_UTF8(bson_doc, "errmsg", "ns not found");
        BSON_APPEND_INT32(bson_doc, "code", 26);
        BSON_APPEND_UTF8(bson_doc, "codeName", "NamespaceNotFound");
        BSON_APPEND_DOUBLE(bson_doc, "ok", 0.0);
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
