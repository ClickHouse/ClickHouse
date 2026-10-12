#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Create.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <algorithm>
#include <string_view>

#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <rapidjson/document.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::MongoProtocol
{

std::vector<Document> CreateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    /// The collection to create is the value of the `create` field of the command itself.
    auto collection = getCollectionRef(documents[0].documents[0], "create");

    /// Everything `createCollection` creates is the same plain placeholder table, while most of its
    /// options - `validator`, `collation`, `capped`, `timeseries`, `viewOn`, `clusteredIndex` and so
    /// on - ask for a collection with semantics of its own. Acknowledging them with `ok: 1` would
    /// promise a validation, an ordering or an eviction that never happens, so any option other
    /// than the generic fields every command may carry is rejected.
    {
        static constexpr std::string_view generic_fields[] = {
            "create", "$db", "lsid", "$clusterTime", "$readPreference", "writeConcern", "comment",
            "apiVersion", "apiStrict", "apiDeprecationErrors", "maxTimeMS"};

        auto json = documents[0].documents[0].getRapidJSONRepresentation();
        for (auto option = json.MemberBegin(); option != json.MemberEnd(); ++option)
        {
            std::string_view name(option->name.GetString(), option->name.GetStringLength());
            if (std::find(std::begin(generic_fields), std::end(generic_fields), name) == std::end(generic_fields))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The option '{}' of the 'create' command is not supported", name);
        }
    }

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
    /// The comment marks the table as that placeholder: the shape of its columns alone is also
    /// the shape of an ordinary table a user may have created, and the rewrite of the first
    /// `insert` must not retype somebody else's table (see `isPlaceholderCollection`).
    executor->execute(fmt::format(
        "CREATE TABLE {} (json JSON) ENGINE = MergeTree ORDER BY tuple() COMMENT {}",
        collection.getQualifiedName(),
        quoteString(PLACEHOLDER_COLLECTION_COMMENT)));

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
