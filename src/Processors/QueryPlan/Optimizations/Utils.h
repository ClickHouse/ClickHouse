#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

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

}
