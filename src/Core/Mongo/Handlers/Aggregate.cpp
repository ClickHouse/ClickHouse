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

#include <functional>
#include <map>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace DB::MongoProtocol
{

namespace
{

/** Removes, in place, every `$unionWith` stage that contributes no documents because it reads a
  * collection that does not exist. Mongo reads a missing collection as empty, and the pipeline of
  * a `$unionWith` over empty input yields nothing - unless that pipeline itself unions another
  * collection, so a nested `$unionWith` is pruned the same way first, and a stage whose nested
  * pipeline still reads another collection is rejected. Without it the translated query would read the
  * missing table and fail with `UNKNOWN_TABLE` instead of treating it as empty.
  * Returns whether the pipeline still reads a collection other than the one the command names.
  */
bool removeUnionsWithMissingCollections(
    rapidjson::Value & pipeline, const std::function<bool(const String &)> & collection_exists, size_t & removed)
{
    if (!pipeline.IsArray())
        return false;

    bool reads_other_collections = false;
    for (auto * stage_it = pipeline.Begin(); stage_it != pipeline.End();)
    {
        if (!stage_it->IsObject())
        {
            ++stage_it;
            continue;
        }

        auto union_it = stage_it->FindMember("$unionWith");
        if (union_it == stage_it->MemberEnd())
        {
            ++stage_it;
            continue;
        }

        const rapidjson::Value * union_collection = nullptr;
        bool nested_reads_other_collections = false;
        if (union_it->value.IsString())
        {
            union_collection = &union_it->value;
        }
        else if (union_it->value.IsObject())
        {
            if (auto collection_it = union_it->value.FindMember("coll"); collection_it != union_it->value.MemberEnd())
                union_collection = &collection_it->value;
            if (auto nested_it = union_it->value.FindMember("pipeline"); nested_it != union_it->value.MemberEnd())
                nested_reads_other_collections = removeUnionsWithMissingCollections(nested_it->value, collection_exists, removed);
        }

        if (union_collection && union_collection->IsString())
        {
            String union_collection_name(union_collection->GetString(), union_collection->GetStringLength());
            if (!collection_exists(union_collection_name))
            {
                /// The same limitation as for a missing aggregated collection: the documents the
                /// nested pipeline unions in cannot be returned without a source to read as empty.
                if (nested_reads_other_collections)
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "The collection '{}' of a '$unionWith' stage with a nested '$unionWith' does not exist: a missing collection is "
                        "read as empty, but the documents of the nested union cannot be returned without it",
                        union_collection_name);

                stage_it = pipeline.Erase(stage_it);
                ++removed;
                continue;
            }
        }

        reads_other_collections = true;
        ++stage_it;
    }

    return reads_other_collections;
}

}

std::vector<Document> AggregateHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & document = documents[0].documents[0];
    auto collection = getCollectionRef(document, "aggregate");

    auto json_representation = document.getRapidJSONRepresentation();
    rejectUnsupportedOptions(json_representation, "aggregate", {"collation", "let"});
    /// `allowDiskUse: true` permits spilling to disk, which the query may or may not do. `false`
    /// forbids it, but ClickHouse spills a large aggregation or sort by default (see
    /// `max_bytes_ratio_before_external_group_by`), so it would answer a stricter request than
    /// it honours.
    if (!getBoolOption(json_representation, "allowDiskUse", "aggregate").value_or(true))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The option 'allowDiskUse: false' of an 'aggregate' command is not supported");
    auto pipeline_it = json_representation.FindMember("pipeline");
    if (pipeline_it == json_representation.MemberEnd() || !pipeline_it->value.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'pipeline' of an 'aggregate' command must be an array of stages");

    /// The reply always holds the whole result in its first batch and closes the cursor (see
    /// `executeSelectIntoCursor`), so `cursor.batchSize` only says how the result is split into
    /// batches, the same as the `batchSize` of a `find` without `singleBatch`. It is still validated.
    if (auto cursor_it = json_representation.FindMember("cursor"); cursor_it != json_representation.MemberEnd())
    {
        if (!cursor_it->value.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'cursor' of an 'aggregate' command must be a document");
        auto batch_size = getWholeNumberOption(cursor_it->value, "batchSize", "aggregate");
        if (batch_size && *batch_size < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'cursor.batchSize' of an 'aggregate' command must not be negative");
    }

    /// A `$match` stage uses the query syntax and is normalized into dotted keys the way the
    /// filter of a `find` is; the rest of the pipeline is left as written, because there a stage
    /// names a nested field with an explicit `a.b` path already, and a nested document is a value
    /// rather than a path.
    /// The database and the collection are passed to the parser separately, so the text names
    /// neither of them, see `MONGO_DIALECT_PLACEHOLDER_NAMESPACE`.
    auto translate = [&](const rapidjson::Value & pipeline)
    {
        auto mongo_dialect_query = fmt::format("{}.aggregate({})", MONGO_DIALECT_PLACEHOLDER_NAMESPACE, serializePipeline(pipeline));

        auto ast = parseMongoDialectCommand(mongo_dialect_query, collection, *executor);

        String sql_query;
        {
            WriteBufferFromString sql_buffer(sql_query);
            ast->format(sql_buffer, IAST::FormatSettings(true));
        }

        /// The settings the pipeline needs are part of the formatted query already, and a second
        /// `SETTINGS` clause would not parse.
        sql_query += " FORMAT JSON";
        return sql_query;
    };

    /// The pipeline is translated as written first, so that a malformed one is an error whether
    /// the collections it reads exist or not.
    String sql_query = translate(pipeline_it->value);

    /// Mongo reads a collection that does not exist as empty rather than raising an error, the same
    /// way `find`, `count` and `distinct` do here. This applies to every collection the pipeline
    /// reads: the aggregated one and the ones of its `$unionWith` stages. The placeholder of
    /// `createCollection` holds no document either, so it is read as empty as well.
    std::map<String, bool> existing_collections;
    auto collection_exists = [&](const String & name)
    {
        auto [it, inserted] = existing_collections.try_emplace(name, false);
        if (inserted)
            it->second = collectionHasSchema(CollectionRef{.database = collection.database, .collection = name}, executor);
        return it->second;
    };

    size_t removed_unions = 0;
    const bool reads_other_collections = removeUnionsWithMissingCollections(pipeline_it->value, collection_exists, removed_unions);

    if (!collection_exists(collection.collection))
    {
        /// A `$unionWith` of an existing collection contributes documents that do not depend on
        /// the collection the command names, so an empty cursor would be the wrong answer. Reading
        /// the aggregated collection as empty while still returning the union would need a source
        /// of the right shape to put in its place, which there is none of, so this combination is
        /// rejected rather than answered incorrectly.
        if (reads_other_collections)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The collection '{}' of an 'aggregate' with a '$unionWith' stage does not exist or has no documents yet: such a "
                "collection is read as empty, but the documents of the union cannot be returned without it",
                collection.getQualifiedName());

        return makeEmptyCursorReply(collection);
    }

    if (removed_unions)
        sql_query = translate(pipeline_it->value);

    return executeSelectIntoCursor(sql_query, collection, executor, getMaxTimeSettings(json_representation, "aggregate"));
}

void registerAggregateHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<AggregateHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
