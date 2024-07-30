#pragma once
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{
using ColumnFilterCreator = std::function<ColumnFilterPtr(const ActionsDAG::Node &)>;
using ColumnFilterCreators = std::vector<ColumnFilterCreator>;

class ColumnFilterHelper
{
public:

    static std::pair<std::vector<ColumnFilterPtr>, std::optional<ActionsDAG>> splitFilterForPushDown(ActionsDAG filter_expression, String filter_name)
    {
        const auto * filter_node = &filter_expression.findInOutputs(filter_name);
        auto conditions = ActionsDAG::extractConjunctionAtoms(filter_node);
        std::vector<ColumnFilterPtr> filters;
        ActionsDAG::NodeRawConstPtrs unsupported_conditions;
        for (const auto * condition : conditions)
        {
            if (std::none_of(creators.begin(), creators.end(), [&](const auto & validator) {
                                ColumnFilterPtr filter =  validator(*condition);
                                filters.push_back(filter);
                                return filter != nullptr;
                            }))
                unsupported_conditions.push_back(condition);
        }

        auto remain_filter = ActionsDAG::buildFilterActionsDAG(unsupported_conditions);
        return {filters, std::move(remain_filter)};
    }

private:
    static ColumnFilterCreators creators;
};
}




