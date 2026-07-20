#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{


/// Returns true if the actions DAG only passes through columns without any transformations
bool isPassthroughActions(const ActionsDAG & actions_dag);

/** Creates a new ExpressionStep or FilterStep node on top of an existing query plan node.
  *  If actions_dag is trivial (only passes through columns), returns original node.
  *  Otherwise creates new ExpressionStep/FilterStep node and adds it to nodes collection.
  *
  *  Typically used when you need to insert a new step before an existing step.
  *  For example, Step1 -> Step2, you want to insert Expression between them: Step1 -> Expression -> Step2.
  *
  *  auto * step2 = step1->children.at(0)
  *  auto * new_node = makeExpressionNodeOnTopOf(step2, std::move(actions), filter_coumn_name, nodes);
  *  step1->children.at(0) = new_node;
  */
QueryPlan::Node * makeExpressionNodeOnTopOf(QueryPlan::Node * node, ActionsDAG actions_dag, const String & filter_column_name, QueryPlan::Nodes & nodes);


}
