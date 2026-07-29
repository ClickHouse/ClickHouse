#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromTextIndexCount.h>

#include <Access/EnabledRowPolicies.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Core/Settings.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/TextIndexUtils.h>

#include <algorithm>

/// Trivial count from the text index: answers `SELECT count() FROM t WHERE <text predicate>` from the index instead of reading data.
/// The pass only rewrites the plan; the index is read at execution time by `ReadFromTextIndexCount`.

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

/// Returns the output column of a bare argument-less count(), or nothing for anything else.
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

/// Collects the text-index virtual columns a predicate DAG reduces to. Fails if any branch is not index-answerable.
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

/// Matches Aggregating -> (Expression|Filter)* -> ReadFromMergeTree and collects its text-predicate columns.
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

/// Guards only proceed when the part-wide cardinalities equal the true row count.
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

using ResolvedQuery = ReadFromTextIndexCount::ResolvedQuery;

/// Recovers the exact-mode text search query for the predicate column from the index read tasks.
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

        return ResolvedQuery{.index = task.index, .condition = std::move(condition), .query = std::move(query)};
    }

    return {};
}

/// E.g. "Trivial count from text index (idx, token = 'alpha')" or "... (idx, tokens = ['alpha', 'zeta'])".
String makeStepDescription(const ResolvedQuery & resolved)
{
    const auto & query_tokens = resolved.query->getTokens();

    WriteBufferFromOwnString description;
    description << "Trivial count from text index (" << resolved.index.index->index.name << ", ";
    if (query_tokens.size() == 1)
    {
        description << "token = '" << query_tokens.front() << "'";
    }
    else
    {
        description << "tokens = [";
        for (size_t i = 0; i < query_tokens.size(); ++i)
            description << (i == 0 ? "'" : ", '") << query_tokens[i] << "'";
        description << "]";
    }
    description << ")";

    return description.str();
}

}

bool optimizeTrivialCountFromTextIndex(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
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

    /// Skip optimization when the text index is partially materialized.
    /// TODO(ahmadov): better handling for the partially materialized text index.
    const auto & parts = matched->reading->getParts();
    const auto & text_index = *resolved->index.index;
    for (const auto & part_with_ranges : parts)
    {
        if (!text_index.getDeserializedFormat(*part_with_ranges.data_part, text_index.getFileName()))
            return false;
    }

    String description = makeStepDescription(*resolved);

    auto & source_node = nodes.emplace_back();
    source_node.step = std::make_unique<ReadFromTextIndexCount>(
        parts,
        std::move(*resolved),
        matched->reading->getReaderSettings(),
        *count_column,
        matched->reading->getNumStreams());
    source_node.step->setStepDescription(description, settings.max_step_description_length);

    aggregating->requestOnlyMergeForAggregateProjection(source_node.step->getOutputHeader());
    node.children.front() = &source_node;

    return true;
}

}
