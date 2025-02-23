#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/ListDatabases.h>

#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <pcg_random.hpp>
#include <Common/CurrentThread.h>
#include <Common/randomSeed.h>

#include <bson/bson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::MongoProtocol
{

std::vector<Document> ListDatabasesHandler::handle(const std::vector<OpMessageSection> &, std::shared_ptr<QueryExecutor> executor)
{
    std::vector<String> result;
    {
        String query = "SHOW DATABASES;";

        auto out = executor->execute(query);
        result = splitByNewline(out);
    }

    bson_t * bson_doc = bson_new();
    bson_t * selected_docs = bson_new();

    String key_identifier = "databases";
    bson_append_array_begin(bson_doc, key_identifier.c_str(), static_cast<Int32>(key_identifier.size()), selected_docs);
    for (size_t i = 0; i < result.size(); ++i)
    {
        auto key_str = std::to_string(i);
        BSON_APPEND_UTF8(selected_docs, key_str.c_str(), result[i].c_str());
    }
    bson_append_array_end(bson_doc, selected_docs);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    Document doc(bson_doc);
    return {doc};
}

void registerListDatabasesHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<ListDatabasesHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
