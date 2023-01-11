#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

using namespace DB;

namespace
{

bool isPartitionKeySuitsGroupByKey(const ReadFromMergeTree & reading, const AggregatingStep & aggregating)
{
    const auto & gb_keys = aggregating.getParams().keys;
    if (aggregating.isGroupingSets() || gb_keys.size() != 1)
        return false;

    const auto & pkey_nodes = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG().getNodes();
    LOG_DEBUG(&Poco::Logger::get("debug"), "{}", reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG().dumpDAG());
    if (!pkey_nodes.empty())
    {
        const auto & func_node = pkey_nodes.back();
        LOG_DEBUG(&Poco::Logger::get("debug"), "{} {} {}", func_node.type, func_node.is_deterministic, func_node.children.size());
        if (func_node.type == ActionsDAG::ActionType::FUNCTION && func_node.function->getName() == "modulo"
            && func_node.children.size() == 2)
        {
            const auto & arg1 = func_node.children.front();
            const auto & arg2 = func_node.children.back();
            LOG_DEBUG(&Poco::Logger::get("debug"), "{} {} {}", arg1->type, arg1->result_name, arg2->type);
            if (arg1->type == ActionsDAG::ActionType::INPUT && arg1->result_name == gb_keys[0]
                && arg2->type == ActionsDAG::ActionType::COLUMN && typeid_cast<const ColumnConst *>(arg2->column.get()))
                return true;
        }
    }

    return false;
}
}

namespace DB::QueryPlanOptimizations
{

size_t tryAggregatePartitionsIndependently(QueryPlan::Node * node, QueryPlan::Nodes &)
{
    if (!node || node->children.size() != 1)
        return 0;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return 0;

    const auto * expression_node = node->children.front();
    if (expression_node->children.size() != 1 || !typeid_cast<const ExpressionStep *>(expression_node->step.get()))
        return 0;

    auto * reading_step = expression_node->children.front()->step.get();
    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_step);
    if (!reading)
        return 0;

    if (!reading->willOutputEachPartitionThroughSeparatePort() && isPartitionKeySuitsGroupByKey(*reading, *aggregating_step))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePort())
            aggregating_step->skipMerging();
    }

    return 0;
}

}
