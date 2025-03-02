#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Index.h>

namespace DB::MongoProtocol
{

std::vector<Document> IndexHandler::handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    auto doc = sections[0].documents[0].getRapidJsonRepresentation();
    const auto & collection = doc["createIndexes"].GetString();
    const auto & array_index = doc["indexes"].GetArray();
    for (auto * it = array_index.begin(); it < array_index.end(); ++it)
    {
        const auto & index_info = it->GetObject()["key"].GetObject();
        for (auto index = index_info.MemberBegin(); index < index_info.MemberEnd(); ++index)
        {
            const auto & column_name = index->name.GetString();
            auto sql_query = fmt::format(
                "ALTER TABLE {} ADD INDEX {} ({}) TYPE bloom_filter(0.02) GRANULARITY 8;", collection, column_name, column_name);
            executor->execute(sql_query);
        }
    }
    bson_t * bson_doc = bson_new();
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document res_doc(bson_doc);
    return {res_doc};
}

void registerIndexHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<IndexHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
