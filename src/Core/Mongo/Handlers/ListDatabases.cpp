#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/ListDatabases.h>

#include <functional>
#include <unordered_set>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>

#include <bson/bson.h>

namespace DB::ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

std::vector<Document> ListDatabasesHandler::handle(
    const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    std::function<bool(const String &)> name_filter;
    bool name_only = false;
    std::optional<bool> authorized_databases;
    {
        auto json = documents[0].documents[0].getRapidJSONRepresentation();
        name_filter = getNameFilter(json, "listDatabases");
        name_only = getBoolOption(json, "nameOnly", "listDatabases").value_or(false);
        authorized_databases = getBoolOption(json, "authorizedDatabases", "listDatabases");
    }

    /// The `listDatabases` privilege of Mongo is a privilege on the whole cluster, i.e. the global
    /// `SHOW DATABASES` grant. Mongo lists every database for a user who has it, and only the
    /// databases the user has some privilege on otherwise, unless asked with
    /// `authorizedDatabases: false` for the full listing, which then needs the privilege. `SHOW DATABASES`
    /// below is filtered by the grants of the user the same way.
    if (authorized_databases == false && !executor->getQueryContext()->getAccess()->isGranted(AccessType::SHOW_DATABASES))
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Not authorized to list all databases: 'authorizedDatabases: false' needs the global 'SHOW DATABASES' grant");

    auto out = executor->execute("SHOW DATABASES");
    auto names = splitByNewline(out);

    /// A database is empty when it has no collection the user can see, as `listCollections` would
    /// return nothing for it.
    std::unordered_set<String> non_empty_databases;
    if (!name_only)
    {
        for (auto & name : splitByNewline(executor->execute("SELECT DISTINCT database FROM system.tables WHERE NOT is_temporary")))
            non_empty_databases.insert(std::move(name));
    }

    bson_t * bson_doc = bson_new();

    {
        static constexpr std::string_view key_identifier = "databases";
        bson_t databases;
        bson_append_array_begin(bson_doc, key_identifier.data(), static_cast<int>(key_identifier.size()), &databases);

        size_t index = 0;
        for (const auto & name : names)
        {
            /// A ClickHouse database whose name is not a legal Mongo database name cannot be used by
            /// any other command, so it is not advertised either.
            if (!isValidMongoDatabaseName(name) || !name_filter(name))
                continue;

            bson_t database_doc;
            bson_init(&database_doc);
            BSON_APPEND_UTF8(&database_doc, "name", name.c_str());
            /// With `nameOnly` a Mongo server answers with the names alone.
            if (!name_only)
                BSON_APPEND_BOOL(&database_doc, "empty", !non_empty_databases.contains(name));

            auto key_str = std::to_string(index);
            ++index;
            bson_append_document(&databases, key_str.c_str(), static_cast<int>(key_str.size()), &database_doc);
            bson_destroy(&database_doc);

            /// The reply is one BSON document, and a reply larger than the `maxBsonObjectSize`
            /// advertised by `isMaster` must be rejected rather than sent: there is no `getMore`
            /// to split it over. The length of the array under construction is live while its
            /// writer is open; the envelope around it is checked below, once the reply is whole.
            if (databases.len > MAX_BSON_OBJECT_SIZE)
            {
                bson_destroy(bson_doc);
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "The list of databases is larger than the largest reply that can be sent ({} bytes)",
                    MAX_BSON_OBJECT_SIZE);
            }
        }

        bson_append_array_end(bson_doc, &databases);
    }
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    if (bson_doc->len > MAX_BSON_OBJECT_SIZE)
    {
        bson_destroy(bson_doc);
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "The list of databases is larger than the largest reply that can be sent ({} bytes)",
            MAX_BSON_OBJECT_SIZE);
    }

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerListDatabasesHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<ListDatabasesHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
