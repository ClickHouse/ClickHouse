#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Count.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <bson/bson.h>
#include <fmt/format.h>

namespace DB::MongoProtocol
{

std::vector<Document> CountHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(documents[0].documents[0], "count");

    auto output = executor->execute(fmt::format("SELECT count() FROM {}", collection.getQualifiedName()));

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", std::stoi(output));
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
