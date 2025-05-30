#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/** Creates a new ExpressionStep or FilterStep node on top of an existing query plan node.
  *  If actions_dag is trivial (only passes through columns), do not touch the node and return false.
  *  Otherwise creates new ExpressionStep/FilterStep node and adds it to nodes collection.
  *
  *  Typically used when you need to insert a new step before an existing step.
  *  For example, Step1 -> Step2, you want to insert Expression between them: Step1 -> Expression -> Step2.
  *
  *  auto & step2 = *step1->children.at(0)
  *  bool changed = makeExpressionNodeOnTopOf(step2, std::move(actions), filter_coumn_name, nodes);
  *  if (changed)
  *      step2->setStepDescription("New Expression");
  */
bool makeExpressionNodeOnTopOf(
    QueryPlan::Node & node, ActionsDAG actions_dag, const String & filter_column_name, QueryPlan::Nodes & nodes,
    std::string_view step_description = {});

}
