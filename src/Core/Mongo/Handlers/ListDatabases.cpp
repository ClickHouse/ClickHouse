#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/ListDatabases.h>

#include <Common/Exception.h>

#include <bson/bson.h>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

std::vector<Document> ListDatabasesHandler::handle(
    const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    bool name_only = false;
    std::optional<String> filter_name;
    {
        auto json = documents[0].documents[0].getRapidJSONRepresentation();

        /** A field that changes what the command answers must be honoured or refused rather than
          * dropped. `nameOnly` is honoured: it asks for the name of each database alone. A
          * `filter` is honoured as an equality on `name` and refused beyond that.
          * `authorizedDatabases` only relaxes which databases the client may list when it lacks
          * the privilege to list them all, and `SHOW DATABASES` already lists exactly the
          * databases the authenticated user is allowed to see, so both of its values are that
          * same listing here.
          */
        static const std::unordered_set<String> supported_fields{"filter", "nameOnly", "authorizedDatabases"};
        rejectUnsupportedCommandFields(json, supported_fields, "listDatabases");
        filter_name = getNameEqualityFilter(json, "listDatabases");
        getBoolOption(json, "authorizedDatabases", "listDatabases");
        name_only = getBoolOption(json, "nameOnly", "listDatabases").value_or(false);
    }

    auto out = executor->execute("SHOW DATABASES");
    auto names = splitByNewline(out);

    /// A database is `empty` when it holds no collection the user can see. The databases that hold
    /// any are listed in one query rather than one probe per database, and only when the reply
    /// carries `empty` at all.
    std::unordered_set<String> non_empty_databases;
    if (!name_only)
    {
        for (auto & database : splitByNewline(executor->execute("SELECT DISTINCT database FROM system.tables WHERE NOT is_temporary")))
            non_empty_databases.insert(std::move(database));
    }

    /// Owned until the reply is built, so a request refused half-way through does not leak it.
    std::unique_ptr<bson_t, decltype(&bson_destroy)> bson_doc_holder(bson_new(), &bson_destroy);
    bson_t * bson_doc = bson_doc_holder.get();

    {
        static constexpr std::string_view key_identifier = "databases";
        bson_t databases;
        bson_append_array_begin(bson_doc, key_identifier.data(), static_cast<int>(key_identifier.size()), &databases);

        size_t index = 0;
        for (const auto & name : names)
        {
            if (name.empty() || (filter_name && name != *filter_name))
                continue;

            bson_t database_doc;
            bson_init(&database_doc);
            BSON_APPEND_UTF8(&database_doc, "name", name.c_str());
            /// With `nameOnly` the reply carries the name of each database and nothing else.
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
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "The list of databases is larger than the largest reply that can be sent ({} bytes)",
            MAX_BSON_OBJECT_SIZE);
    }

    std::vector<Document> result;
    result.emplace_back(bson_doc_holder.release());
    return result;
}

void registerListDatabasesHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<ListDatabasesHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
