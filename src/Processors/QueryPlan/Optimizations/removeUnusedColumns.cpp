#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{
namespace QueryPlanOptimizations
{

size_t tryRemoveUnusedColumns(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
	auto& parent = node->step;

	auto updated = false;

	const auto& parent_inputs =  parent->getInputHeaders();
	for (auto child_id = 0U; child_id < node->children.size(); ++child_id)
	{
		auto & child_step  = node->children[child_id]->step;
		if (!child_step->canRemoveUnusedColumns())
			continue;

		const auto & required_outputs = parent_inputs[child_id];
			updated |= child_step->removeUnusedColumns(required_outputs.getNames());
	}

	return updated ? 2 : 0;
}

}
}
