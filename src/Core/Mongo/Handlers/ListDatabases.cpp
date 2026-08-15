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

std::vector<Document> ListDatabasesHandler::handle(const std::vector<OpMessageSection> &, std::shared_ptr<QueryExecutor> executor)
{
    auto out = executor->execute("SHOW DATABASES");
    auto names = splitByNewline(out);

    bson_t * bson_doc = bson_new();

    {
        static constexpr std::string_view key_identifier = "databases";
        bson_t databases;
        bson_append_array_begin(bson_doc, key_identifier.data(), static_cast<int>(key_identifier.size()), &databases);

        size_t index = 0;
        for (const auto & name : names)
        {
            if (name.empty())
                continue;

            bson_t database_doc;
            bson_init(&database_doc);
            BSON_APPEND_UTF8(&database_doc, "name", name.c_str());
            BSON_APPEND_BOOL(&database_doc, "empty", false);

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
