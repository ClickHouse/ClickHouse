#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Create.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

namespace DB::MongoProtocol
{

std::vector<Document> CreateHandler::handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session)
{
    const auto & request_doc = documents[0].documents[0];
    auto query_context = session->makeQueryContext();
    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    query_context->setCurrentQueryId(fmt::format("mongo:{:d}", secret_key));

    auto json = request_doc.getRapidJsonRepresentation();
    String table_name = json["drop"].GetString();

    {
        auto query = fmt::format("CREATE TABLE IF NOT EXISTS {} (json JSON) ENGINE = Memory SETTINGS enable_json_type = 1;", table_name);

        CurrentThread::QueryScope query_scope{query_context};
        ReadBufferFromString read_buf(query);

        WriteBufferFromOwnString out;
        executeQuery(read_buf, out, false, query_context, {});
    }

    bson_t * bson_doc = bson_new();
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc, true);
    return {doc};
}

void registerCreateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<CreateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
