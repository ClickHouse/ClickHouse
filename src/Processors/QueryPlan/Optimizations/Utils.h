#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <type_traits>

namespace DB
{

class ActionsDAG;

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

[[nodiscard]] FilterResult filterResultForNotMatchedRows(
    const ActionsDAG & filter_dag,
    const String & filter_column_name,
    const Block & input_stream_header,
    bool allow_unknown_function_arguments = false);

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
