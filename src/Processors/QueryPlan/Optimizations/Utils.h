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

/// Add a filter that removes rows for which all columns expanded by an inner `ARRAY JOIN` are empty.
/// The condition is `length(c1) > 0 OR ... OR length(cn) > 0`, so rows with unequal non-zero array
/// sizes still reach an aligned `ARRAY JOIN` and raise `SIZES_OF_ARRAYS_DONT_MATCH`.
/// Column names are taken from `array_join`; source names are resolved via `ArrayJoinStep::getSourceColumnName`.
/// The lookup also tries the joined column names themselves, so the filter can be built both immediately
/// below the step (where the header uses analyzer aliases) and further down the input (where original
/// column names remain). The step input header is used to recover constant ARRAY JOIN expressions that
/// do not need to be read from `input_node`.
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
