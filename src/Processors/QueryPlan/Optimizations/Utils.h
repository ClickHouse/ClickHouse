#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <type_traits>

namespace DB
{

class ActionsDAG;
class ArrayJoinStep;
class ReadFromMergeTree;
class SortingStep;

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

/// True if optimizeExchanges will lift a plain gather above this step, so a scatter/gather pair separated
/// by it still collapses. Shared with findGatherOverRead, which has to predict that rewrite.
[[nodiscard]] bool canHoistGatherThroughStep(const IQueryPlanStep & step);

[[nodiscard]] FilterResult filterResultForNotMatchedRows(
    const ActionsDAG & filter_dag,
    const String & filter_column_name,
    const Block & input_stream_header,
    bool allow_unknown_function_arguments = false);

[[nodiscard]] FilterResult filterResultForMatchedRows(
    ActionsDAG pre_actions_dag,
    const ActionsDAG & filter_dag,
    const String & filter_column_name);

/// Walk down a chain of `ExpressionStep`s below a sort, rewriting `description` so that its
/// column names refer to the input level of the deepest step reached. `node` is advanced past
/// every peeled step.
///
/// For each sort column we look up the output node by name and walk through any `ALIAS` chain -
/// if it ends at an `INPUT` node, the column is a pure pass-through and we replace its name with
/// the input's name. Anything else (FUNCTION, COLUMN, ARRAY_JOIN, ...) means the sort key was
/// computed in this step rather than carried over, so pushing the sort below it would be unsound
/// and we return `false`. An `arrayJoin` anywhere in the expression also returns `false`, because
/// it changes the number of rows per input row (see `#82279`).
///
/// `max_peel` bounds the walk. A cap of 4 is generous: in current plans the only steps between
/// `Sorting` and a row-multiplying step after `mergeExpressions` are `Before ORDER BY +
/// Projection` and `Post Join Actions`, occasionally with one more wrapper.
///
/// Returns `false` when the caller must abandon the rewrite; `node` and `description` may then
/// have been partially advanced and must not be used.
[[nodiscard]] bool peelPassThroughExpressions(QueryPlan::Node *& node, SortDescription & description, size_t max_peel = 4);

/// Walk down a single-child chain looking for a `ReadFromMergeTree` step. Used by top-K
/// pushdowns that must cooperate with parallel replicas and `optimizeReadInOrder`.
const ReadFromMergeTree * findMergeTreeRead(const QueryPlan::Node * node);

/// True when inserting a materializing `Sort + Limit` above `input_node` must be abandoned:
/// parallel-replica coordination would conflict with a local top-n, or (when
/// `defer_to_read_in_order`) the second-pass `optimizeReadInOrder` can already stream the
/// requested order - including the `FINAL` + descending-key case that pass 2 rejects even
/// when `wouldReadInOrderBeUseful` says yes.
[[nodiscard]] bool shouldSkipTopKAboveMergeTreeInput(
    const QueryPlan::Node & input_node,
    const SortingStep & sort_step,
    const SortDescription & description,
    size_t limit,
    bool defer_to_read_in_order);

/// Add a filter that removes rows for which all columns expanded by an inner `ARRAY JOIN` are empty.
/// The condition is `length(c1) > 0 OR ... OR length(cn) > 0`, so rows with unequal non-zero array
/// sizes still reach an aligned `ARRAY JOIN` and raise `SIZES_OF_ARRAYS_DONT_MATCH`.
///
/// Must be built on the immediate input of `array_join`: joined columns are present there under
/// the names in `array_join.getColumns()`, and constant arrays are folded by `ActionsDAG` itself.
///
/// `input_node` is updated to point to the inserted filter. If the condition is constant, no node
/// is added because limiting the input cannot change whether a constant `ARRAY JOIN` emits rows.
/// Returns false only if the condition cannot be constructed.
[[nodiscard]] bool addArrayJoinEmptinessFilter(
    ArrayJoinStep & array_join,
    QueryPlan::Node *& input_node,
    QueryPlan::Nodes & nodes);

struct NoOp
{
};

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
