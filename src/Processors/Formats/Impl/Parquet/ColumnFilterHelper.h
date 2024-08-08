#pragma once
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>

namespace DB
{
using ColumnFilterCreator = std::function<OptionalFilter(const ActionsDAG::Node &)>;
using ColumnFilterCreators = std::vector<ColumnFilterCreator>;

struct FilterSplitResult
{
    std::unordered_map<String, std::vector<ColumnFilterPtr>> filters;
    std::optional<ActionsDAG> remain_filter;
};

class ColumnFilterHelper
{
public:

    static FilterSplitResult splitFilterForPushDown(const ActionsDAG& filter_expression)
    {
        const auto * filter_node = filter_expression.getOutputs().front();
        auto conditions = ActionsDAG::extractConjunctionAtoms(filter_node);
        std::vector<ColumnFilterPtr> filters;
        ActionsDAG::NodeRawConstPtrs unsupported_conditions;
        FilterSplitResult split_result;
        for (const auto * condition : conditions)
        {
            if (std::none_of(creators.begin(), creators.end(), [&](ColumnFilterCreator & creator) {
                                auto result = creator(*condition);
                                if (result.has_value())
                                    split_result.filters[result.value().first].emplace_back(result.value().second);
                                return result.has_value();
                            }))
                unsupported_conditions.push_back(condition);
        }
        if (!unsupported_conditions.empty())
        {
            auto remain_filter = ActionsDAG::buildFilterActionsDAG(unsupported_conditions);
            split_result.remain_filter = std::move(remain_filter);
        }
        return split_result;
    }

private:
    static ColumnFilterCreators creators;
};

void pushFilterToParquetReader(const ActionsDAG& filter_expression, ParquetReader & reader);
}




