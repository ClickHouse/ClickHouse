#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Delete.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <bson/bson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<Document> DeleteHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    if (documents.size() < 2 || documents[1].documents.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'delete' command does not contain any filter");

    auto collection = getCollectionRef(documents[0].documents[0], "delete");
    const auto & filter_doc = documents[1].documents[0];

    String serialized_filter;
    {
        auto json_representation = filter_doc.getRapidJSONRepresentation();
        auto filter_it = json_representation.FindMember("q");
        if (filter_it == json_representation.MemberEnd())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'delete' command does not contain the 'q' filter");

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        filter_it->value.Accept(writer);
        serialized_filter = buffer.GetString();
    }
    serialized_filter = modifyFilter(serialized_filter);

    auto mongo_dialect_query = fmt::format("db.{}.deleteMany({})", collection.collection, serialized_filter);

    auto parser = Mongo::ParserMongoQuery(10000, 10000, 10000);
    auto ast = Mongo::parseMongoQuery(
        parser,
        mongo_dialect_query.data(),
        mongo_dialect_query.data() + mongo_dialect_query.size(),
        "",
        10000,
        10000,
        10000,
        collection.database);

    String sql_query;
    {
        WriteBufferFromString sql_buffer(sql_query);
        ast->format(sql_buffer, IAST::FormatSettings(true));
    }

    executor->execute(sql_query);

    bson_t * bson_doc = bson_new();

    BSON_APPEND_INT32(bson_doc, "n", 0);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerDeleteHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<DeleteHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
