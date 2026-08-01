#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Aggregate.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/parseMongoQuery.h>

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

std::vector<Document> AggregateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = documents[0].documents[0];
    auto collection = getCollectionRef(document, "aggregate");

    auto json_representation = document.getRapidJSONRepresentation();
    auto pipeline_it = json_representation.FindMember("pipeline");
    if (pipeline_it == json_representation.MemberEnd() || !pipeline_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'pipeline' of an 'aggregate' command must be an array of stages");

    String serialized_pipeline;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        pipeline_it->value.Accept(writer);
        serialized_pipeline = buffer.GetString();
    }

    /// Unlike the filter of a `find`, a pipeline is not rewritten into dotted keys: a stage names
    /// a nested field with an explicit `a.b` path already, and a nested document inside a stage is
    /// a value rather than a path.
    ///
    /// The database is passed to the parser separately, so that a collection named in the query
    /// text as `db.<collection>` keeps the text independent of the database name, which may itself
    /// be `db`.
    auto mongo_dialect_query = fmt::format("db.{}.aggregate({})", collection.collection, serialized_pipeline);

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

    /// The settings the pipeline needs are part of the formatted query already, and a second
    /// `SETTINGS` clause would not parse.
    sql_query += " FORMAT JSON";

    return executeSelectIntoCursor(sql_query, collection, executor);
}

void registerAggregateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<AggregateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
