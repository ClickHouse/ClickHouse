#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Index.h>

#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
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
        if (!index.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "An index of the 'createIndexes' command must be a document");

        /** The only semantics this handler implements is a single-field index that may speed up equality
          * filters (a `bloom_filter` data skipping index). Everything else - uniqueness, compound keys,
          * TTL, special index types - is a contract the server cannot honor, so acknowledging it with
          * `ok: 1` would silently change the behavior the application relies on. Reject anything unknown.
          */
        for (auto option = index.MemberBegin(); option != index.MemberEnd(); ++option)
        {
            String option_name = option->name.GetString();
            if (option_name != "key" && option_name != "name" && option_name != "v")
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "The index option '{}' of the 'createIndexes' command is not supported",
                    option_name);
        }

        auto key_it = index.FindMember("key");
        if (key_it == index.MemberEnd() || !key_it->value.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "An index of the 'createIndexes' command has no 'key'");

        if (key_it->value.MemberCount() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'key' of an index of the 'createIndexes' command is empty");

        if (key_it->value.MemberCount() > 1)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Compound indexes are not supported, create one index per field");

        for (auto column = key_it->value.MemberBegin(); column != key_it->value.MemberEnd(); ++column)
        {
            if (!column->value.IsInt64() || (column->value.GetInt64() != 1 && column->value.GetInt64() != -1))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "The index on '{}' is not supported: the value of a 'key' field must be 1 or -1",
                    column->name.GetString());

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
