#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Source with KeyCondition to push down filters.
class SourceWithKeyCondition : public ISource
{
protected:
    /// Represents pushed down filters in source
    std::shared_ptr<const KeyCondition> key_condition;

    void setKeyConditionImpl(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context, const Block & keys)
    {
        key_condition = std::make_shared<const KeyCondition>(
            filter_actions_dag ? &*filter_actions_dag : nullptr,
            context,
            keys.getNames(),
            std::make_shared<ExpressionActions>(ActionsDAG(keys.getColumnsWithTypeAndName())));
    }

public:
    using Base = ISource;
    using Base::Base;

    /// Set key_condition directly. It is used for filter push down in source.
    virtual void setKeyCondition(const std::shared_ptr<const KeyCondition> & key_condition_) { key_condition = key_condition_; }

    /// Set key_condition created by filter_actions_dag and context.
    virtual void setKeyCondition(const std::optional<ActionsDAG> & /*filter_actions_dag*/, ContextPtr /*context*/) { }
};
}
