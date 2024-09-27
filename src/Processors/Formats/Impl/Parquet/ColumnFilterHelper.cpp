#include "ColumnFilterHelper.h"

namespace DB
{

ColumnFilterCreators ColumnFilterHelper::creators = {BigIntRangeFilter::create, NegatedBigIntRangeFilter::create, createFloatRangeFilter, ByteValuesFilter::create, NegatedByteValuesFilter::create};
FilterSplitResult ColumnFilterHelper::splitFilterForPushDown(const ActionsDAG & filter_expression)
{
    if (filter_expression.getOutputs().empty())
        return {};
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
    for (auto & condition : unsupported_conditions)
    {
        auto actions_dag = ActionsDAG::buildFilterActionsDAG({condition});
        if (actions_dag.has_value())
        {
            split_result.expression_filters.emplace_back(std::make_shared<ExpressionFilter>(std::move(actions_dag.value())));
        }
    }
    return split_result;
}

void pushFilterToParquetReader(const ActionsDAG& filter_expression, ParquetReader & reader)
{
    if (filter_expression.getOutputs().empty()) return ;
    auto split_result = ColumnFilterHelper::splitFilterForPushDown(std::move(filter_expression));
    for (const auto & item : split_result.filters)
    {
        for (const auto& filter: item.second)
        {
            reader.addFilter(item.first, filter);
        }
    }
    for (auto & expression_filter : split_result.expression_filters)
    {
        reader.addExpressionFilter(expression_filter);
    }
}
}
