#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/Insert.h>
#include "Core/Mongo/Document.h"
#include "IO/WriteBufferFromString.h"

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include "Common/Exception.h"
#include <Common/CurrentThread.h>
#include <Common/randomSeed.h>


namespace DB::MongoProtocol
{

void InsertHandler::createDatabase(const Document & doc, std::shared_ptr<QueryExecutor> executor)
{
    auto json = doc.getRapidJsonRepresentation();

    String dbname = json["$db"].GetString();
    {
        auto query = fmt::format("CREATE DATABASE IF NOT EXISTS {};", dbname);
        executor->execute(query);
    }

    {
        auto query = fmt::format("USE {}", dbname);
        executor->execute(query);
    }
}

String InsertHandler::createTable(const Document & doc, std::shared_ptr<QueryExecutor> executor)
{
    auto json = doc.getRapidJsonRepresentation();
    String table_name = json["insert"].GetString();

    {
        auto query = fmt::format("CREATE TABLE IF NOT EXISTS {} (json JSON) ENGINE = Memory SETTINGS enable_json_type = 1;", table_name);

        executor->execute(query);
    }
    return table_name;
}

std::vector<Document> InsertHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto table_name = createTable(documents[0].documents[0], executor);

    size_t inserted_count = 0;
    for (size_t section_id = 1; section_id < documents.size(); ++section_id)
    {
        const auto & docs = documents[section_id].documents;
        for (const auto & doc : docs)
        {
            ++inserted_count;
            auto query = fmt::format("INSERT INTO {} VALUES ('{}')", table_name, doc.getJson());
            executor->execute(query);
        }
    }

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", inserted_count);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc, true);
    return {doc};
}

void registerInsertHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<InsertHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
