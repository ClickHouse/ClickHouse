#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/IsMaster.h>

namespace DB::MongoProtocol
{

std::vector<Document> IsMasterHandler::handle(const std::vector<OpMessageSection> &, std::shared_ptr<QueryExecutor>)
{
    bson_t * bson_doc = bson_new();

    BSON_APPEND_BOOL(bson_doc, "ismaster", true);
    BSON_APPEND_BOOL(bson_doc, "isWritablePrimary", true);
    BSON_APPEND_INT32(bson_doc, "maxBsonObjectSize", static_cast<int32_t>(MAX_BSON_OBJECT_SIZE));
    BSON_APPEND_INT32(bson_doc, "maxMessageSizeBytes", static_cast<int32_t>(MAX_MESSAGE_SIZE));
    BSON_APPEND_INT32(bson_doc, "maxWriteBatchSize", 100000);
    /// `localTime` is a BSON date rather than a number: a driver reads it as the time of the
    /// server, and an integer of the same milliseconds is a different wire type.
    BSON_APPEND_DATE_TIME(bson_doc, "localTime", static_cast<int64_t>(time(nullptr)) * 1000);
    BSON_APPEND_INT32(bson_doc, "logicalSessionTimeoutMinutes", 30);
    BSON_APPEND_INT32(bson_doc, "minWireVersion", 0);
    BSON_APPEND_INT32(bson_doc, "maxWireVersion", 19);
    BSON_APPEND_BOOL(bson_doc, "readOnly", false);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerIsMasterHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<IsMasterHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
