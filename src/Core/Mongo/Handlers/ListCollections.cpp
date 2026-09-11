#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/ListCollections.h>

#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <bson/bson.h>
#include <rapidjson/document.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
}

namespace DB::MongoProtocol
{

std::vector<Document> ListCollectionsHandler::handle(
    const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    /// `listCollections` names no collection, only the database it applies to.
    String database;
    {
        auto json = documents[0].documents[0].getRapidJSONRepresentation();
        auto database_it = json.FindMember("$db");
        if (database_it == json.MemberEnd() || !database_it->value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'listCollections' command does not contain the '$db' database name");
        database = database_it->value.GetString();
    }

    /// A database that does not exist has no collections, as in Mongo, where listing them
    /// returns an empty cursor rather than an error.
    std::vector<std::string> names;
    if (objectExists(executor, "DATABASE", backQuoteIfNeed(database)))
        names = splitByNewline(executor->execute(fmt::format("SHOW TABLES FROM {}", backQuoteIfNeed(database))));

    /// The reply is `{"cursor": {"firstBatch": [...], "id": 0, "ns": "<db>.$cmd.listCollections"}, "ok": 1}`.
    bson_t cursor;
    bson_init(&cursor);

    {
        static constexpr std::string_view key_identifier = "firstBatch";
        bson_t first_batch;
        bson_append_array_begin(&cursor, key_identifier.data(), static_cast<int>(key_identifier.size()), &first_batch);

        size_t index = 0;
        for (const auto & name : names)
        {
            if (name.empty())
                continue;

            bson_t collection_doc;
            bson_init(&collection_doc);

            bson_t options_doc;
            bson_init(&options_doc);
            bson_t id_index_doc;
            bson_init(&id_index_doc);
            bson_t info;
            bson_init(&info);
            BSON_APPEND_BOOL(&info, "readOnly", false);

            BSON_APPEND_UTF8(&collection_doc, "name", name.c_str());
            BSON_APPEND_UTF8(&collection_doc, "type", "collection");
            BSON_APPEND_DOCUMENT(&collection_doc, "options", &options_doc);
            BSON_APPEND_DOCUMENT(&collection_doc, "idIndex", &id_index_doc);
            BSON_APPEND_DOCUMENT(&collection_doc, "info", &info);

            bson_destroy(&options_doc);
            bson_destroy(&id_index_doc);
            bson_destroy(&info);

            auto key_str = std::to_string(index);
            ++index;
            bson_append_document(&first_batch, key_str.c_str(), static_cast<int>(key_str.size()), &collection_doc);
            bson_destroy(&collection_doc);

            /// The reply is one BSON document, and a reply larger than the `maxBsonObjectSize`
            /// advertised by `isMaster` must be rejected rather than sent: there is no `getMore`
            /// to split it over. The length of the batch under construction is live while its
            /// writer is open; the envelope around it is checked below, once the reply is whole.
            if (first_batch.len > MAX_BSON_OBJECT_SIZE)
            {
                bson_destroy(&cursor);
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "The list of collections is larger than the largest reply that can be sent ({} bytes)",
                    MAX_BSON_OBJECT_SIZE);
            }
        }

        bson_append_array_end(&cursor, &first_batch);
    }
    BSON_APPEND_INT64(&cursor, "id", 0);
    String namespace_name = database + ".$cmd.listCollections";
    BSON_APPEND_UTF8(&cursor, "ns", namespace_name.c_str());

    bson_t * result_doc = bson_new();
    BSON_APPEND_DOCUMENT(result_doc, "cursor", &cursor);
    BSON_APPEND_DOUBLE(result_doc, "ok", 1.0);
    bson_destroy(&cursor);

    if (result_doc->len > MAX_BSON_OBJECT_SIZE)
    {
        bson_destroy(result_doc);
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "The list of collections is larger than the largest reply that can be sent ({} bytes)",
            MAX_BSON_OBJECT_SIZE);
    }

    std::vector<Document> result;
    result.emplace_back(result_doc);
    return result;
}

void registerListCollectionsHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<ListCollectionsHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
