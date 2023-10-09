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

    void setKeyConditionImpl(const SelectQueryInfo & query_info, ContextPtr context, const Block & keys)
    {
        if (!context->getSettingsRef().allow_experimental_analyzer)
        {
            key_condition = std::make_shared<const KeyCondition>(
                query_info,
                context,
                keys.getNames(),
                std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(keys.getColumnsWithTypeAndName())));
        }
    }

    void setKeyConditionImpl(const ActionsDAG::NodeRawConstPtrs & nodes, ContextPtr context, const Block & keys)
    {
        if (context->getSettingsRef().allow_experimental_analyzer)
        {
            std::unordered_map<std::string, DB::ColumnWithTypeAndName> node_name_to_input_column;
            for (const auto & column : keys.getColumnsWithTypeAndName())
                node_name_to_input_column.insert({column.name, column});

            auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(nodes, node_name_to_input_column, context);
            key_condition = std::make_shared<const KeyCondition>(
                filter_actions_dag,
                context,
                keys.getNames(),
                std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(keys.getColumnsWithTypeAndName())),
                NameSet{});
        }
    }

public:
    using Base = ISource;
    using Base::Base;

    /// If the format is used by a SELECT query, this method may be called.
    /// The source may use it for filter pushdown.
    virtual void setKeyCondition(const std::shared_ptr<const KeyCondition> & key_condition_) { key_condition = key_condition_; }

    /// It is used for filter push down when allow_experimental_analyzer is false.
    virtual void setKeyCondition(const SelectQueryInfo & /*query_info*/, ContextPtr /*context*/) { }

    /// It is used for filter push down when allow_experimental_analyzer is true
    virtual void setKeyCondition(const ActionsDAG::NodeRawConstPtrs & /*nodes*/, ContextPtr /*context*/) { }
};
}
