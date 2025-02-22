#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/ListCollections.h>

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include <Common/CurrentThread.h>
#include <Common/randomSeed.h>
#include "Core/Mongo/Document.h"

#include <bson/bson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::MongoProtocol
{

std::vector<Document> ListCollectionsHandler::handle(const std::vector<OpMessageSection> &, std::unique_ptr<Session> & session)
{
    auto query_context = session->makeQueryContext();
    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    std::vector<bson_t *> result;
    {
        String query = "SHOW TABLES;";

        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(query);

        WriteBufferFromOwnString out;
        executeQuery(read_buf, out, false, query_context, {});

        auto names = splitByNewline(out.str());

        for (const auto & name : names)
        {
            bson_t * collection_doc = bson_new();
            bson_t * options_doc = bson_new();
            bson_t * id_index_doc = bson_new();
            bson_t * info = bson_new();

            BSON_APPEND_BOOL(info, "readOnly", false);

            BSON_APPEND_UTF8(collection_doc, "name", name.c_str());
            BSON_APPEND_UTF8(collection_doc, "type", "collection");
            BSON_APPEND_DOCUMENT(collection_doc, "options", options_doc);
            BSON_APPEND_DOCUMENT(collection_doc, "idIndex", id_index_doc);
            BSON_APPEND_DOCUMENT(collection_doc, "info", info);

            result.push_back(collection_doc);
        }
    }

    bson_t * cursor_doc = bson_new();
    bson_t * selected_docs = bson_new();

    {
        String key_identifier = "firstBatch";
        bson_append_array_begin(selected_docs, key_identifier.c_str(), static_cast<Int32>(key_identifier.size()), cursor_doc);
        for (size_t i = 0; i < result.size(); ++i)
        {
            auto key_str = std::to_string(i);
            BSON_APPEND_DOCUMENT(cursor_doc, key_str.c_str(), result[i]);
        }
        bson_append_array_end(selected_docs, cursor_doc);
    }
    BSON_APPEND_INT64(selected_docs, "id", 0);
    String table_id = "db";
    BSON_APPEND_UTF8(selected_docs, "ns", table_id.c_str());

    bson_t * result_doc = bson_new();
    BSON_APPEND_DOCUMENT(result_doc, "cursor", selected_docs);
    BSON_APPEND_DOUBLE(result_doc, "ok", 1.0);

    Document doc(result_doc, true);
    return {doc};
}

void registerListCollectionsHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<ListCollectionsHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
