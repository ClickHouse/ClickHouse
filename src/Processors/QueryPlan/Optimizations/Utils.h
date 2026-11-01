#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <optional>
#include <type_traits>

namespace DB
{

class AggregatingStep;
struct AggregateDescription;

struct IDescriptionHolder
{
    virtual void setStepDescription(IQueryPlanStep & step) const = 0;
    virtual ~IDescriptionHolder() = default;
};

using DescriptionHolderPtr = std::unique_ptr<const IDescriptionHolder>;

class DescriptionHolder : public IDescriptionHolder
{
public:
    template <size_t size>
    ALWAYS_INLINE explicit DescriptionHolder(const char (&description_)[size]) : description(description_, size - 1) {}

    void setStepDescription(IQueryPlanStep & step) const override
    {
        step.step_description = description;
    }

private:
    std::string_view description;
};

template <size_t size>
ALWAYS_INLINE DescriptionHolderPtr makeDescription(const char (&description)[size])
{
    return std::make_unique<DescriptionHolder>(description);
}

/** Creates a new ExpressionStep or FilterStep node on top of an existing query plan node.
  *  If actions_dag is trivial (only passes through columns), do not touch the node and return false.
  *  Otherwise creates new ExpressionStep/FilterStep node and adds it to nodes collection.
  *
  *  Typically used when you need to insert a new step before an existing step.
  *  For example, Step1 -> Step2, you want to insert Expression between them: Step1 -> Expression -> Step2.
  *
  *  auto & step2 = *step1->children.at(0)
  *  bool changed = makeExpressionNodeOnTopOf(step2, std::move(actions), nodes);
  */
bool makeExpressionNodeOnTopOf(
    QueryPlan::Node & node, ActionsDAG actions_dag, QueryPlan::Nodes & nodes,
    DescriptionHolderPtr step_description = {});

bool makeFilterNodeOnTopOf(
    QueryPlan::Node & node, ActionsDAG actions_dag, const String & filter_column_name, bool remove_filer, QueryPlan::Nodes & nodes,
    DescriptionHolderPtr step_description = {});

bool isPassthroughActions(const ActionsDAG & actions_dag);

namespace QueryPlanOptimizations
{

enum class FilterResult
{
    UNKNOWN,
    TRUE,
    FALSE,
};

[[nodiscard]] FilterResult getFilterResult(const ColumnWithTypeAndName & column);

[[nodiscard]] bool dagContainsNonReadySet(const ActionsDAG & dag);

[[nodiscard]] bool dagContainsNonDeterministicFunction(const ActionsDAG & dag);

/// True when the expression holds a function that is non-deterministic within the query (`rand`), stateful
/// (`rowNumberInAllBlocks`, `aiEmbed`), or has observable side effects (`sleep`), so an observer can see how
/// many times and on which rows it was evaluated.
[[nodiscard]] bool isSensitiveToEvaluationCount(const ActionsDAG & dag);

/// Whether another node of `dag` has the result name of `node`. The reader and the PREWHERE read steps find columns by
/// name, so another node under the same name would be taken for this one.
[[nodiscard]] bool hasResultNameClash(const ActionsDAG & dag, const ActionsDAG::Node * node);

/// True if optimizeExchanges will lift a plain gather above this step, so a scatter/gather pair separated
/// by it still collapses. Shared with findGatherOverRead, which has to predict that rewrite.
[[nodiscard]] bool canHoistGatherThroughStep(const IQueryPlanStep & step);

/// A top-K of the groups of an aggregation by one of its aggregates (see `findAggregationTopK`).
struct AggregationTopK
{
    QueryPlan::Node * aggregating_node = nullptr;
    AggregatingStep * aggregating = nullptr;
    /// The groups that the top takes: the limit of the sorting, `OFFSET` included.
    size_t limit = 0;
    /// The position of the aggregate that the top ranks by.
    size_t rank_index = 0;
    bool ascending = false;
    /// Whether the sorting compares the aggregate by a collation.
    bool has_collator = false;
};

/// The top-K of the groups of an aggregation that `limit_node` takes, if the plan has the shape of one there: a
/// `LimitStep` without `WITH TIES` or an exact `rows_before_limit_at_least`, over a `SortingStep` with a limit by one
/// column without `WITH FILL`, over expressions that pass the column on unchanged and keep the rows of every group,
/// over a final aggregation by keys, not in order, without grouping sets or an overflow row, of which the column is an
/// aggregate.
[[nodiscard]] std::optional<AggregationTopK> findAggregationTopK(QueryPlan::Node & limit_node);

/// Whether an aggregate never exceeds the rows of its group: a lone `count`, or the exact distinct count `uniqExact` or
/// `uniqExactIf`.
[[nodiscard]] bool isRowBoundedCount(const AggregateDescription & aggregate);

[[nodiscard]] FilterResult filterResultForNotMatchedRows(
    const ActionsDAG & filter_dag,
    const String & filter_column_name,
    const Block & input_stream_header,
    bool allow_unknown_function_arguments = false);

[[nodiscard]] FilterResult filterResultForMatchedRows(
    ActionsDAG pre_actions_dag,
    const ActionsDAG & filter_dag,
    const String & filter_column_name);

struct NoOp
{
};

/// Is this step a wrapper that can be skipped over? Only an `ExpressionStep` whose outputs are a
/// permutation of its inputs - renamed or reordered, nothing computed, nothing forwarded twice.
///
/// The renames are the point, so this cannot be `isPassthroughActions` above: that one wants
/// `getOutputs() == getInputs()`, and a rename makes the output an `ALIAS` node rather than the `INPUT`
/// it stands for. Every analyzer query is bracketed by such steps - `Change column names to column
/// identifiers` on the way in, `Project names` on the way out - so demanding identity here means never
/// recognising a wrapper at all.
///
/// Its two users have to agree on it, which is why it is shared: `calculateHashTableCacheKeys` lets such
/// a step adopt its child's key, and `considerEnablingParallelReplicas` looks through it when locating
/// the boundary the replicas would ship from. A step invisible to one and visible to the other would be
/// instrumented in one plan and matched in the other. It is narrower than "contributes nothing to the
/// key": a full `SortingStep` contributes nothing yet must remain a boundary of its own.
bool isPassthroughExpressionWithRenames(const IQueryPlanStep & step);

/// Whether a MergeTree read allows a second, row-addressed read of the rows it selected, which lazy materialization
/// needs. Shared by both lazy materialization paths, so that they agree on it.
bool canUseLazyMaterializationForReadingStep(ReadFromMergeTree * reading);

template <typename Func1, typename Func2 = NoOp>
void traverseQueryPlan(Stack & stack, QueryPlan::Node & root, Func1 && on_enter, Func2 && on_leave = {})
{
    stack.clear();
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if constexpr (!std::is_same_v<Func1, NoOp>)
        {
            if (frame.next_child == 0)
            {
                on_enter(*frame.node);
            }
        }

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        if constexpr (!std::is_same_v<Func2, NoOp>)
        {
            on_leave(*frame.node);
        }

        stack.pop_back();
    }
}

}
}
