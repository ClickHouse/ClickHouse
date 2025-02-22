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

void InsertHandler::createDatabase(const Document & doc, std::unique_ptr<Session> & session)
{
    auto json = doc.getRapidJsonRepresentation();
    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto query_context = session->makeQueryContext();

    String dbname = json["$db"].GetString();
    {
        auto query = fmt::format("CREATE DATABASE IF NOT EXISTS {};", dbname);
        auto secret_key = dis(gen);
        query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

        std::cerr << "query " << query << '\n';

        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(query);

        WriteBufferFromOwnString out;
        executeQuery(read_buf, out, false, query_context, {});
    }

    {
        auto query = fmt::format("USE {}", dbname);
        auto secret_key = dis(gen);
        query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(query);

        WriteBufferFromOwnString out;
        executeQuery(read_buf, out, false, query_context, {});
    }
}

String InsertHandler::createTable(const Document & doc, std::unique_ptr<Session> & session)
{
    auto query_context = session->makeQueryContext();
    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    auto json = doc.getRapidJsonRepresentation();
    String table_name = json["insert"].GetString();

    {
        auto query = fmt::format("CREATE TABLE IF NOT EXISTS {} (json JSON) ENGINE = Memory SETTINGS enable_json_type = 1;", table_name);

        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(query);

        WriteBufferFromOwnString out;
        executeQuery(read_buf, out, false, query_context, {});
    }
    return table_name;
}

/*
parsed doc { "insert" : "test_collection", "ordered" : true, "lsid" : { "id" : { "$binary" : "+bHLHvxWTzigGuptRfqAGQ==", "$type" : "04" } }, "$db" : "test_database" }
kind 1
READ IDENTIFIER documents 223 9 237
rest_available 0
parsed doc { "_id" : { "$oid" : "67b4f53c410d8b0035eaf5b2" }, "name" : "Bob Johnson", "age" : 32, "city" : "New York" }
parsed doc { "_id" : { "$oid" : "67b4f53c410d8b0035eaf5b3" }, "name" : "Charlie Brown", "age" : 24, "city" : "Los Angeles" }
parsed doc { "_id" : { "$oid" : "67b4f53c410d8b0035eaf5b4" }, "name" : "David Williams", "age" : 40, "city" : "Chicago" }
*/
std::vector<Document> InsertHandler::handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session)
{
    std::cerr << "handle insert start\n";
    //createDatabase(documents[0].documents[0], session);
    std::cerr << "handle insert bp1\n";

    auto table_name = createTable(documents[0].documents[0], session);
    std::cerr << "handle insert bp2 " << table_name << '\n';

    size_t inserted_count = 0;
    for (size_t section_id = 1; section_id < documents.size(); ++section_id)
    {
        const auto & docs = documents[section_id].documents;
        for (const auto & doc : docs)
        {
            ++inserted_count;
            auto query_context = session->makeQueryContext();
            pcg64_fast gen{randomSeed()};
            std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
            auto secret_key = dis(gen);

            query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

            auto query = fmt::format("INSERT INTO {} VALUES ('{}')", table_name, doc.getJson());
            std::cerr << "exec query " << query << '\n';
            CurrentThread::QueryScope query_scope{query_context};
            ReadBufferFromString read_buf(query);

            WriteBufferFromOwnString out;
            executeQuery(read_buf, out, false, query_context, {});
        }
    }
    std::cerr << "Insert OK\n";

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
