#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/TextIndexUtils.h>

/// Trivial count from the text index: answers `SELECT count() FROM t WHERE <text predicate>` from the
/// index's stored per-token row counts (`TokenPostingsInfo::cardinality`) instead of reading data. When
/// the subtree above a `ReadFromMergeTree` is a bare `count()` over a pure text-index predicate, it is
/// replaced with a `ReadFromPreparedSource` holding a pre-filled count() state, mirroring the exact-count
/// projection path in `optimizeUseAggregateProjection.cpp`.
///
/// First cut: single-token `hasToken`/`hasAnyTokens`/`hasAllTokens`, Exact direct-read mode, materialized
/// index, no residual predicate / FINAL / SAMPLE / mutations. `hasPhrase` and multi-token AND/OR need
/// posting decoding and are follow-ups.

namespace DB
{
namespace Setting
{
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool optimize_trivial_count_query;
}
}

namespace DB::QueryPlanOptimizations
{

namespace
{

/// The count() output column if `aggregating` is a bare `count()`/`count(*)` (no GROUP BY, one aggregate,
/// no arguments), else nullopt. Mirrors the detection in getAggregateProjectionCandidates.
std::optional<String> matchBareCount(const AggregatingStep & aggregating)
{
    if (aggregating.isGroupingSets())
        return {};

    const auto & params = aggregating.getParams();
    if (!params.keys.empty() || params.aggregates.size() != 1)
        return {};

    const auto & desc = params.aggregates.front();

    /// count(col) counts non-nulls of a column; only argument-less count() qualifies.
    if (!desc.argument_names.empty())
        return {};

    if (!typeid_cast<const AggregateFunctionCount *>(desc.function.get()))
        return {};

    return desc.column_name;
}

/// True iff the filter column's expression is composed only of text-index virtual-column inputs joined by
/// `and` (through transparent alias/cast wrappers), collecting those columns; any other node returns false.
/// Matches the plan after `processAndOptimizeTextIndexFunctions` has turned `hasToken(...)` into an
/// `__text_index_...` column.
bool collectTextIndexPredicateColumns(const ActionsDAG::Node * node, NameSet & out_columns)
{
    switch (node->type)
    {
        case ActionsDAG::ActionType::ALIAS:
            return collectTextIndexPredicateColumns(node->children.front(), out_columns);

        case ActionsDAG::ActionType::INPUT:
            if (isTextIndexVirtualColumn(node->result_name))
            {
                out_columns.insert(node->result_name);
                return true;
            }
            return false;

        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!node->function_base)
                return false;

            const auto & name = node->function_base->getName();

            if (name == "and")
            {
                for (const auto * child : node->children)
                    if (!collectTextIndexPredicateColumns(child, out_columns))
                        return false;
                return true;
            }

            /// Transparent wrappers that do not change which rows pass.
            if ((name == "_CAST" || name == "CAST") && node->children.size() >= 1)
                return collectTextIndexPredicateColumns(node->children.front(), out_columns);

            /// TODO: OR of text-index predicates (needs posting-list union cardinality).
            return false;
        }

        default:
            return false;
    }
}

struct MatchedSubtree
{
    ReadFromMergeTree * reading = nullptr;
    QueryPlan::Node * read_node = nullptr;
    /// Text-index virtual columns gating the read (from FilterSteps and PREWHERE).
    NameSet predicate_columns;
};

/// Walks AggregatingStep -> ReadFromMergeTree through only Expression/Filter steps; every FilterStep (and
/// the read's PREWHERE) must be a pure text-index predicate. nullopt if the shape does not match.
std::optional<MatchedSubtree> matchSubtree(QueryPlan::Node & aggregating_node)
{
    MatchedSubtree matched;

    QueryPlan::Node * current = &aggregating_node;
    while (true)
    {
        if (current->children.size() != 1)
            return {};

        QueryPlan::Node * child = current->children.front();
        IQueryPlanStep * step = child->step.get();

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            matched.reading = reading;
            matched.read_node = child;
            break;
        }

        if (auto * filter = typeid_cast<FilterStep *>(step))
        {
            const ActionsDAG & dag = filter->getExpression();
            const auto * filter_node = &dag.findInOutputs(filter->getFilterColumnName());
            if (!collectTextIndexPredicateColumns(filter_node, matched.predicate_columns))
                return {};
        }
        else if (!typeid_cast<ExpressionStep *>(step))
            return {};

        current = child;
    }

    /// The text predicate often lives in PREWHERE after PREWHERE optimization.
    if (const auto & prewhere = matched.reading->getPrewhereInfo())
    {
        const auto * prewhere_node = &prewhere->prewhere_actions.findInOutputs(prewhere->prewhere_column_name);
        if (!collectTextIndexPredicateColumns(prewhere_node, matched.predicate_columns))
            return {};
    }

    /// Without a text-index predicate this is a plain count already handled by trivial/minmax count.
    if (matched.predicate_columns.empty())
        return {};

    return matched;
}

/// Correctness guards; any false => bail to the normal read. These mirror the trivial-count framework's
/// opt-outs. Row policies / residual predicates need no check: they appear as non-text filter conjuncts
/// that `matchSubtree` already rejects.
bool guardsHold(const ReadFromMergeTree & reading)
{
    auto context = reading.getContext();

    /// Mutations, patch parts and lightweight deletes change row visibility, so index-time cardinalities
    /// would disagree. Read the mutations snapshot directly: `MergeTreeData::supportsTrivialCountOptimization`
    /// dereferences the read step's (null) storage-snapshot data and would segfault here.
    if (const auto & mutations = reading.getMutationsSnapshot();
        mutations && (mutations->hasDataMutations() || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask()))
        return false;

    /// Each parallel replica would independently sum all parts -> N-times overcount.
    if (reading.isParallelReadingFromReplicas())
        return false;

    /// A transaction may see Outdated parts that the cardinalities do not reflect.
    if (context->getCurrentTransaction())
        return false;

    /// An empty set must then yield an empty result, not a 0 row.
    if (context->getSettingsRef()[Setting::empty_result_for_aggregation_by_empty_set])
        return false;

    if (reading.isQueryWithFinal() || reading.isQueryWithSampling())
        return false;

    if (reading.getParts().empty())
        return false;

    /// A token's cardinality is part-wide (one index granule per part), so it equals the selected-granule
    /// count only if no granule holding the token was pruned. Text-index pruning drops only token-free
    /// granules (safe), but PK/minmax pruning can drop matching ones -> require none happened...
    auto analysis = reading.getAnalyzedResult();
    if (!analysis || analysis->total_marks_pk != analysis->selected_marks_pk)
        return false;

    /// ...and that every skip index that pruned is a text index, leaving the text predicate as the only pruner.
    const auto & indexes = reading.getIndexes();
    if (!indexes)
        return false;

    for (const auto & useful : indexes->skip_indexes.useful_indices)
        if (!useful.index->isTextIndex())
            return false;

    return true;
}

struct ResolvedQuery
{
    MergeTreeIndexWithCondition index;
    std::shared_ptr<MergeTreeIndexConditionText> condition;
    TextSearchQueryPtr query;
};

/// Resolves the matched virtual column back to its text-search query, or nullopt unless there is exactly
/// one predicate resolving to a single-token, Exact-mode, non-phrase, non-pattern query -- the only shape
/// whose count equals a sum of posting cardinalities. The `virtual_column -> query` map is read back from
/// the condition retained on the read step's index read tasks. A multi-predicate AND would need the row-set
/// intersection, not a sum, so we bail (follow-up).
std::optional<ResolvedQuery> recoverSearchQuery(const ReadFromMergeTree & reading, const NameSet & predicate_columns)
{
    if (predicate_columns.size() != 1)
        return {};

    const String & column_name = *predicate_columns.begin();

    for (const auto & [index_name, task] : reading.getIndexReadTasks())
    {
        if (!task.index.condition_template)
            continue;

        auto condition = std::dynamic_pointer_cast<MergeTreeIndexConditionText>(task.index.condition_template->generateUnsubstituted());
        if (!condition)
            continue;

        auto query = condition->getSearchQueryForVirtualColumn(column_name);
        if (!query)
            continue;

        /// Hint mode keeps the original predicate, so only Exact is answerable from the index alone.
        if (query->getDirectReadMode() != TextIndexDirectReadMode::Exact)
            return {};

        /// Phrase needs positions; pattern/LIKE needs a posting scan.
        if (query->getSearchMode() == TextSearchMode::Phrase || !query->getPatterns().empty())
            return {};

        if (query->getTokens().size() != 1)
            return {};

        return ResolvedQuery{.index = task.index, .condition = std::move(condition), .query = std::move(query)};
    }

    return {};
}

/// Loads a part's text index granule (mirrors `MergeTreeReaderTextIndex::readGranule`) to read the
/// dictionary-resident token cardinalities. nullptr if the index is not materialized in the part.
MergeTreeIndexGranulePtr loadTextIndexGranuleForPart(
    const IMergeTreeDataPart & part,
    const MergeTreeIndexWithCondition & index,
    const MergeTreeIndexConditionText & condition,
    const MergeTreeReaderSettings & reader_settings)
{
    auto index_format = index.index->getDeserializedFormat(part.checksums, index.index->getFileName(), &part.getDataPartStorage());
    if (!index_format)
        return nullptr;

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = &condition,
        .part = part,
        .index = *index.index,
        .readable_ranges = nullptr,
    };

    const auto substreams = index.index->getSubstreams();
    auto make_stream = [&](const MergeTreeIndexSubstream & substream)
    {
        return makeTextIndexInputStream(
            part.getDataPartStoragePtr(),
            index.index->getFileName() + substream.suffix,
            substream.extension,
            MergeTreeIndexReader::patchSettings(reader_settings, substream.type));
    };

    auto sparse_index_stream = make_stream(substreams[0]);
    auto dictionary_stream = make_stream(substreams[1]);
    auto postings_stream = make_stream(substreams[2]);

    sparse_index_stream->seekToStart();

    MergeTreeIndexInputStreams streams;
    streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexDictionary] = dictionary_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexPostings] = postings_stream.get();

    auto granule = index.index->createIndexGranule();
    granule->deserializeBinaryWithMultipleStreams(streams, state);
    return granule;
}

/// Exact matching-row count for `resolved` in one part from the token's dictionary cardinality, or nullopt
/// if it cannot be answered from the index (the caller then abandons the optimization).
std::optional<UInt64> computeCountForPart(
    const RangesInDataPart & part_with_ranges,
    const ResolvedQuery & resolved,
    const MergeTreeReaderSettings & reader_settings)
{
    const auto & data_part = part_with_ranges.data_part;

    /// Part-wide cardinality is exact here: guardsHold established the text predicate was the only pruner.
    auto granule = loadTextIndexGranuleForPart(*data_part, resolved.index, *resolved.condition, reader_settings);
    if (!granule)
        return {};

    auto text_granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(granule);
    if (!text_granule)
        return {};

    const auto & token_infos = text_granule->getAnalyzer().getAllTokenInfos();

    const auto & token = resolved.query->getTokens().front();
    auto it = token_infos.find(token);
    if (it == token_infos.end())
        return 0;

    return it->second->cardinality;
}

/// A ReadFromPreparedSource emitting one pre-filled count() state named `count_column`.
QueryPlanStepPtr makeCountSource(UInt64 count, const String & count_column)
{
    auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});

    Block block_with_count{
        {createSingleCountStateColumn(agg_count, count),
         std::make_shared<DataTypeAggregateFunction>(agg_count, DataTypes{}, Array{}),
         count_column}};

    Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(block_with_count))));
    return std::make_unique<ReadFromPreparedSource>(std::move(pipe));
}

}

bool optimizeTrivialCountFromTextIndex(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    auto log = getLogger("optimizeTrivialCountFromTextIndex");

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return false;

    auto count_column = matchBareCount(*aggregating);
    if (!count_column)
    {
        LOG_TRACE(log, "bail: not a bare count()");
        return false;
    }

    auto matched = matchSubtree(node);
    if (!matched)
    {
        LOG_TRACE(log, "bail: not a pure text-index predicate over ReadFromMergeTree");
        return false;
    }

    /// Kill switch: honour the trivial-count family's parent setting instead of adding our own (also
    /// implicitly gated by direct read, which produces the `__text_index_...` column we match).
    if (!matched->reading->getContext()->getSettingsRef()[Setting::optimize_trivial_count_query])
    {
        LOG_TRACE(log, "bail: optimize_trivial_count_query = 0");
        return false;
    }

    if (!guardsHold(*matched->reading))
    {
        LOG_TRACE(log, "bail: guardsHold not satisfied (modifiers / mutations / parallel replicas / non-text pruning)");
        return false;
    }

    auto resolved = recoverSearchQuery(*matched->reading, matched->predicate_columns);
    if (!resolved)
    {
        LOG_TRACE(log, "bail: not a single-token Exact non-phrase query");
        return false;
    }

    const auto & reader_settings = matched->reading->getReaderSettings();

    UInt64 total_count = 0;
    for (const auto & part_with_ranges : matched->reading->getParts())
    {
        auto part_count = computeCountForPart(part_with_ranges, *resolved, reader_settings);
        if (!part_count)
        {
            LOG_TRACE(log, "bail: cannot count part '{}' exactly", part_with_ranges.data_part->name);
            return false;
        }
        total_count += *part_count;
    }

    LOG_DEBUG(log, "Answered count() = {} from text index", total_count);

    auto & source_node = nodes.emplace_back();
    source_node.step = makeCountSource(total_count, *count_column);
    source_node.step->setStepDescription("Trivial count from text index", settings.max_step_description_length);

    /// Feed the precomputed state to the aggregate as a merge-only input.
    aggregating->requestOnlyMergeForAggregateProjection(source_node.step->getOutputHeader());
    node.children.front() = &source_node;

    return true;
}

}
