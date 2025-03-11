#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>

namespace DB
{

class KeyCondition;
class ActionsDAG;

/// Source with KeyCondition to push down filters.
class SourceWithKeyCondition : public ISource
{
protected:
    /// Represents pushed down filters in source
    std::shared_ptr<const KeyCondition> key_condition;

    void setKeyConditionImpl(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context, const Block & keys);

public:
    using Base = ISource;
    using Base::Base;

    /// Set key_condition directly. It is used for filter push down in source.
    virtual void setKeyCondition(const std::shared_ptr<const KeyCondition> & key_condition_) { key_condition = key_condition_; }

    /// Set key_condition created by filter_actions_dag and context.
    virtual void setKeyCondition(const std::optional<ActionsDAG> & /*filter_actions_dag*/, ContextPtr /*context*/) { }
};
}
