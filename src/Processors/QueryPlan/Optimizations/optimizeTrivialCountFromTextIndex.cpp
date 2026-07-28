#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Access/EnabledRowPolicies.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Core/Settings.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/TextIndexUtils.h>

#include <algorithm>

/// Trivial count from the text index: answers `SELECT count() FROM t WHERE <text predicate>` from the index instead of reading data.
///
/// A single-token predicate uses the metadata `TokenPostingsInfo::cardinality`.
/// A multi-token `hasAllTokens`/`hasAnyTokens` combines the per-token posting lists (intersection / union) and takes the cardinality.

namespace DB
{
namespace Setting
{
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
}
}

namespace DB::QueryPlanOptimizations
{

namespace
{

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
            if ((name == "_CAST" || name == "CAST") && !node->children.empty())
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

bool guardsHold(const ReadFromMergeTree & reading)
{
    auto context = reading.getContext();

    /// Each parallel replica would independently sum all parts -> N-times overcount.
    if (reading.isParallelReadingFromReplicas())
        return false;

    if (reading.getDistributedReadBucketCount() > 0)
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

    auto analysis = reading.getAnalyzedResult();
    if (!analysis || analysis->total_marks_pk != analysis->selected_marks_pk)
        return false;

    const auto & indexes = reading.getIndexes();
    if (!indexes)
        return false;

    for (const auto & useful : indexes->skip_indexes.useful_indices)
        if (!useful.index->isTextIndex())
            return false;

    /// Row policy filters rows the cardinality ignores; without a database name it can't be resolved, so fail closed.
    auto storage_id = reading.getStorageID();
    if (!storage_id.hasDatabase())
        return false;

    if (auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        row_policy_filter && !row_policy_filter->isAlwaysTrue())
        return false;

    if (const auto & mutations = reading.getMutationsSnapshot();
        mutations && (mutations->hasDataMutations() || mutations->hasPatchParts() || mutations->hasLightweightDeletedMask()))
        return false;

    if (reading.getStorageMetadata()->hasUniqueKey())
        return false;

    return true;
}

struct ResolvedQuery
{
    MergeTreeIndexWithCondition index;
    std::shared_ptr<MergeTreeIndexConditionText> condition;
    TextSearchQueryPtr query;
};

std::optional<ResolvedQuery> recoverSearchQuery(const ReadFromMergeTree & reading, const NameSet & predicate_columns)
{
    if (predicate_columns.size() != 1)
        return {};

    const String & column_name = *predicate_columns.begin();

    for (const auto & [index_name, task] : reading.getIndexReadTasks())
    {
        /// Only the task that produced this virtual column can resolve it.
        bool owns_column = std::ranges::any_of(task.columns, [&column_name](const auto & column) { return column.name == column_name; });
        if (!owns_column || !task.index.condition_template)
            continue;

        auto condition = std::dynamic_pointer_cast<MergeTreeIndexConditionText>(task.index.condition_template->generateUnsubstituted());
        if (!condition)
            continue;

        auto query = condition->getSearchQueryForVirtualColumn(column_name);

        /// Hint mode keeps the original predicate, so only Exact is answerable from the index alone.
        if (query->getDirectReadMode() != TextIndexDirectReadMode::Exact)
            return {};

        /// Phrase needs positions; pattern/LIKE needs a posting scan.
        if (query->getSearchMode() == TextSearchMode::Phrase || !query->getPatterns().empty())
            return {};

        if (query->getTokens().empty())
            return {};

        /// Multi-token intersection would eagerly read common tokens' postings the reader skips lazily (regression).
        /// TODO(ahmadov): handle the multi-token ALL/AND case.
        if (query->getTokens().size() > 1 && query->getSearchMode() == TextSearchMode::All)
            return {};

        return ResolvedQuery{.index = task.index, .condition = std::move(condition), .query = std::move(query)};
    }

    return {};
}

std::optional<UInt64> computeCountForPart(
    const RangesInDataPart & part_with_ranges,
    const ResolvedQuery & resolved,
    const MergeTreeReaderSettings & reader_settings)
{
    const auto & data_part = part_with_ranges.data_part;
    const auto & index = resolved.index;

    auto index_format = index.index->getDeserializedFormat(*data_part, index.index->getFileName());
    if (!index_format)
        return {};

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = resolved.condition.get(),
        .part = *data_part,
        .index = *index.index,
        .readable_ranges = nullptr,
    };

    const auto substreams = index.index->getSubstreams();
    auto make_stream = [&](const MergeTreeIndexSubstream & substream)
    {
        return makeTextIndexInputStream(
            data_part->getDataPartStoragePtr(),
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

    auto granule_ptr = index.index->createIndexGranule();
    granule_ptr->deserializeBinaryWithMultipleStreams(streams, state);
    auto granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(granule_ptr);

    if (!granule)
        return {};

    const auto & analyzer = granule->getAnalyzer();
    const auto & tokens = resolved.query->getTokens();

    /// One granule per part (GRANULARITY ignored), so the dictionary cardinality is the exact part-wide count; no posting I/O.
    if (tokens.size() == 1)
    {
        const auto & token_infos = analyzer.getAllTokenInfos();
        auto it = token_infos.find(tokens.front());
        return it == token_infos.end() ? 0 : static_cast<UInt64>(it->second->cardinality);
    }

    /// `is_failed`: e.g. All mode with a token missing from the part. An empty part matches nothing.
    if (data_part->rows_count == 0)
        return 0;

    const auto & query_builder = analyzer.getQueryBuilder(*resolved.query);
    if (query_builder.is_failed)
        return 0;

    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(granule->getPostingsCodecType()),
        granule->getSerializationVersion());
    const RowsRange full_range(0, data_part->rows_count - 1);
    const bool intersect = resolved.query->getSearchMode() == TextSearchMode::All;

    std::optional<PostingList> result;
    for (const auto & [token, token_info] : query_builder.tokens)
    {
        PostingList token_postings;
        if (token_info->embedded_postings)
        {
            token_postings = *token_info->embedded_postings;
        }
        else
        {
            for (size_t block_idx : token_info->getBlocksToRead(full_range))
            {
                auto block = MergeTreeIndexGranuleText::readPostingsBlock(
                    *postings_stream, state, *token_info, block_idx, postings_serialization, granule->getIndexIdForCaches());
                if (block)
                    token_postings |= *block;
            }
        }

        if (!result)
            result = std::move(token_postings);
        else if (intersect)
            *result &= token_postings;
        else
            *result |= token_postings;
    }

    return result ? result->cardinality() : 0;
}

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
    if (!settings.optimize_trivial_count_from_text_index)
        return false;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return false;

    auto count_column = matchBareCount(*aggregating);
    if (!count_column)
        return false;

    auto matched = matchSubtree(node);
    if (!matched)
        return false;

    if (!matched->reading->getAnalyzedResult())
        matched->reading->setAnalyzedResult(matched->reading->selectRangesToRead());

    if (!guardsHold(*matched->reading))
        return false;

    auto resolved = recoverSearchQuery(*matched->reading, matched->predicate_columns);
    if (!resolved)
        return false;

    const auto & reader_settings = matched->reading->getReaderSettings();

    /// Reading the per-part index is real plan-time work; keep it interruptible for tables with many parts.
    auto query_status = matched->reading->getContext()->getProcessListElementSafe();

    UInt64 total_count = 0;
    for (const auto & part_with_ranges : matched->reading->getParts())
    {
        if (query_status)
            query_status->checkTimeLimit();

        auto part_count = computeCountForPart(part_with_ranges, *resolved, reader_settings);
        if (!part_count)
            return false;
        total_count += *part_count;
    }

    const auto & query_tokens = resolved->query->getTokens();
    String quoted_tokens;
    for (const auto & token : query_tokens)
        quoted_tokens += fmt::format("{}'{}'", quoted_tokens.empty() ? "" : ", ", token);
    String tokens_desc = query_tokens.size() == 1
        ? fmt::format("token = {}", quoted_tokens)
        : fmt::format("tokens = [{}]", quoted_tokens);

    auto & source_node = nodes.emplace_back();
    source_node.step = makeCountSource(total_count, *count_column);
    source_node.step->setStepDescription(
        fmt::format("Trivial count from text index ({}, {})", resolved->index.index->index.name, tokens_desc),
        settings.max_step_description_length);

    aggregating->requestOnlyMergeForAggregateProjection(source_node.step->getOutputHeader());
    node.children.front() = &source_node;

    return true;
}

}
