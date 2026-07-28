#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Index.h>

#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<Document> IndexHandler::handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor)
{
    auto collection = getCollectionRef(sections[0].documents[0], "createIndexes");

    auto doc = sections[0].documents[0].getRapidJSONRepresentation();
    auto indexes_it = doc.FindMember("indexes");
    if (indexes_it == doc.MemberEnd() || !indexes_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'createIndexes' command does not contain the 'indexes' array");

    for (const auto & index : indexes_it->value.GetArray())
    {
        auto key_it = index.FindMember("key");
        if (key_it == index.MemberEnd() || !key_it->value.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "An index of the 'createIndexes' command has no 'key'");

        for (auto column = key_it->value.MemberBegin(); column != key_it->value.MemberEnd(); ++column)
        {
            String column_name = column->name.GetString();
            auto sql_query = fmt::format(
                "ALTER TABLE {} ADD INDEX IF NOT EXISTS {} ({}) TYPE bloom_filter(0.02) GRANULARITY 8",
                collection.getQualifiedName(),
                backQuoteIfNeed(column_name),
                backQuoteIfNeed(column_name));
            executor->execute(sql_query);
        }
    }

    bson_t * bson_doc = bson_new();
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerIndexHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<IndexHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
