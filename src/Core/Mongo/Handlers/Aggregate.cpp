#include <Core/Mongo/Document.h>
#include <Core/Mongo/DocumentCollectionShape.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Aggregate.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Parsers/Mongo/DocumentCollection.h>
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
extern const int NOT_IMPLEMENTED;
}

namespace DB::MongoProtocol
{

namespace
{

/// Whether any stage of the pipeline, including the ones nested in a `$unionWith`, reads a
/// collection other than the one the command names.
bool pipelineReadsOtherCollections(const rapidjson::Value & pipeline)
{
    if (!pipeline.IsArray())
        return false;

    for (const auto & stage : pipeline.GetArray())
    {
        if (!stage.IsObject())
            continue;

        auto union_it = stage.FindMember("$unionWith");
        if (union_it == stage.MemberEnd())
            continue;
        if (!union_it->value.IsObject())
            return true;

        if (auto nested_it = union_it->value.FindMember("pipeline"); nested_it != union_it->value.MemberEnd())
            if (pipelineReadsOtherCollections(nested_it->value))
                return true;
        return true;
    }

    return false;
}

/// A query-wide rewrite is correct only when every source collection has the same physical shape.
/// Reject heterogeneous `$unionWith` sources rather than rewriting a branch through the other
/// collection's `json` column.
void checkUnionCollectionShapes(
    const rapidjson::Value & pipeline, const CollectionShape & shape, const String & database, std::shared_ptr<QueryExecutor> executor)
{
    if (!pipeline.IsArray())
        return;

    /// A collection that does not exist is read as empty and has no shape to differ by: the
    /// aggregated one is reported as missing by the handler, and a missing union collection
    /// contributes no documents.
    if (!shape.exists)
        return;

    for (const auto & stage : pipeline.GetArray())
    {
        if (!stage.IsObject())
            continue;
        auto union_it = stage.FindMember("$unionWith");
        if (union_it == stage.MemberEnd())
            continue;

        String union_collection;
        const rapidjson::Value * nested_pipeline = nullptr;
        if (union_it->value.IsString())
            union_collection = {union_it->value.GetString(), union_it->value.GetStringLength()};
        else if (union_it->value.IsObject())
        {
            auto collection_it = union_it->value.FindMember("coll");
            if (collection_it != union_it->value.MemberEnd() && collection_it->value.IsString())
                union_collection = {collection_it->value.GetString(), collection_it->value.GetStringLength()};
            if (auto pipeline_it = union_it->value.FindMember("pipeline"); pipeline_it != union_it->value.MemberEnd())
                nested_pipeline = &pipeline_it->value;
        }

        if (!union_collection.empty())
        {
            CollectionRef union_ref{database, union_collection};
            const auto union_shape = getCollectionShape(union_ref, executor);
            if (union_shape.exists && union_shape.stores_documents != shape.stores_documents)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "An 'aggregate' with '$unionWith' collections of different storage shapes is not supported");
        }
        if (nested_pipeline)
            checkUnionCollectionShapes(*nested_pipeline, shape, database, executor);
    }
}

}

std::vector<Document> AggregateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = documents[0].documents[0];
    auto collection = getCollectionRef(document, "aggregate");

    auto json_representation = document.getRapidJSONRepresentation();

    /// The pipeline is what an `aggregate` answers; the other fields it may carry either name how it
    /// is executed (`allowDiskUse`, `hint`), which changes nothing about the answer, or are not
    /// implemented and are refused rather than dropped.
    static const std::unordered_set<String> supported_fields{"pipeline", "cursor", "allowDiskUse", "hint"};
    rejectUnsupportedCommandFields(json_representation, supported_fields, "aggregate");

    /// Every document of the result is answered in the first batch, so a `cursor` that asks for a
    /// `batchSize` asks for a batching this handler does not do.
    if (auto cursor_it = json_representation.FindMember("cursor"); cursor_it != json_representation.MemberEnd())
    {
        static const std::unordered_set<String> supported_cursor_fields{};
        rejectUnsupportedFields(cursor_it->value, supported_cursor_fields, "cursor", "aggregate");
    }

    auto pipeline_it = json_representation.FindMember("pipeline");
    if (pipeline_it == json_representation.MemberEnd() || !pipeline_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'pipeline' of an 'aggregate' command must be an array of stages");

    /// A `$match` stage uses the query syntax and is normalized into dotted keys the way the
    /// filter of a `find` is; the rest of the pipeline is left as written, because there a stage
    /// names a nested field with an explicit `a.b` path already, and a nested document is a value
    /// rather than a path.
    auto serialized_pipeline = serializePipeline(pipeline_it->value);

    /// The database is passed to the parser separately, so that a collection named in the query
    /// text as `db.<collection>` keeps the text independent of the database name, which may itself
    /// be `db`.
    auto mongo_dialect_query = fmt::format("db.{}.aggregate({})", collection.collection, serialized_pipeline);

    const auto max_query_size = mongo_dialect_query.size();
    /// The reparse runs under the parser limits of the session, the same ones the Mongo dialect
    /// parses under, so that the wire endpoint accepts exactly what the dialect accepts.
    const auto limits = executor->getParserLimits();
    auto parser = Mongo::ParserMongoQuery(max_query_size, limits.max_parser_depth, limits.max_parser_backtracks);
    auto ast = Mongo::parseMongoQuery(
        parser,
        mongo_dialect_query.data(),
        mongo_dialect_query.data() + mongo_dialect_query.size(),
        "",
        max_query_size,
        limits.max_parser_depth,
        limits.max_parser_backtracks,
        collection.database);

    /// A collection of documents addresses its fields as the paths of the document column, and a
    /// pipeline that ends without building documents of its own answers with the stored ones.
    const auto shape = getCollectionShape(collection, executor);
    checkUnionCollectionShapes(pipeline_it->value, shape, collection.database, executor);
    bool holds_documents = false;
    if (shape.stores_documents)
    {
        Mongo::rewriteFieldsAsDocumentPaths(ast);
        holds_documents = Mongo::selectDocumentsOfCollection(ast);
    }

    String sql_query;
    {
        WriteBufferFromString sql_buffer(sql_query);
        ast->format(sql_buffer, IAST::FormatSettings(true));
    }

    /// Mongo reads a collection that does not exist as empty rather than raising an error, the same
    /// way `find`, `count` and `distinct` do here. The pipeline is translated first, so that a
    /// malformed one is still an error.
    if (!objectExists(executor, "TABLE", collection.getQualifiedName()))
    {
        /// A `$unionWith` reads a collection of its own, and the documents it contributes do not
        /// depend on the collection the command names, so an empty cursor would be the wrong
        /// answer. Reading the aggregated collection as empty while still returning the union
        /// would need a source of the right shape to put in its place, which there is none of,
        /// so this combination is rejected rather than answered incorrectly.
        if (pipelineReadsOtherCollections(pipeline_it->value))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The collection '{}' of an 'aggregate' with a '$unionWith' stage does not exist: a missing collection is read as empty, "
                "but the documents of the union cannot be returned without it",
                collection.getQualifiedName());

        return makeEmptyCursorReply(collection);
    }

    return executeSelectIntoCursor(sql_query, collection, executor, holds_documents);
}

void registerAggregateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<AggregateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
