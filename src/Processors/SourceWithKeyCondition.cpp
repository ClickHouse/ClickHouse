#include <Processors/SourceWithKeyCondition.h>

#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

void SourceWithKeyCondition::setKeyConditionImpl(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context, const Block & keys)
{
    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr, context);
    key_condition = std::make_shared<const KeyCondition>(
        inverted_dag,
        context,
        keys.getNames(),
        std::make_shared<ExpressionActions>(ActionsDAG(keys.getColumnsWithTypeAndName())));
}

}
