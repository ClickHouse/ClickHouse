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
    ActionsDAGPtr filter_dag;
    ContextPtr ctx;

    void setKeyConditionImpl(const ActionsDAGPtr & filter_actions_dag, ContextPtr context_, const Block & keys)
    {
        setContext(context_);
        setActionsDag(filter_actions_dag);
        key_condition = std::make_shared<const KeyCondition>(
            filter_actions_dag,
            ctx,
            keys.getNames(),
            std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(keys.getColumnsWithTypeAndName())));
    }

public:
    using Base = ISource;
    using Base::Base;

    void setActionsDag(const ActionsDAGPtr & filter_actions_dag_)
    {
        filter_dag = filter_actions_dag_;
    }

    void setContext(ContextPtr context_)
    {
        ctx = context_;
    }

    /// Set key_condition directly. It is used for filter push down in source.
    virtual void setKeyCondition(const std::shared_ptr<const KeyCondition> & key_condition_) { key_condition = key_condition_; }

    /// Set key_condition created by filter_actions_dag and context.
    virtual void setKeyCondition(const ActionsDAGPtr & /*filter_actions_dag*/, ContextPtr /*context*/) { }
};
}
