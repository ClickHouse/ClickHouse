#include "ColumnFilterHelper.h"
#include <Poco/String.h>

namespace DB
{

// TODO: support more expressions
ColumnFilterCreators ColumnFilterHelper::creators
    = {BigIntRangeFilter::create,
       NegatedBigIntRangeFilter::create,
       createFloatRangeFilter,
       BytesValuesFilter::create,
       NegatedByteValuesFilter::create};

FilterSplitResult ColumnFilterHelper::splitFilterForPushDown(const ActionsDAG & filter_expression, bool case_insensitive)
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
        // convert expr to column filter, and try to merge with existing filter on same column
        if (std::none_of(
                creators.begin(),
                creators.end(),
                [&](ColumnFilterCreator & creator)
                {
                    auto result = creator(*condition);
                    if (result.has_value())
                    {
                        auto col_name = result.value().first;
                        if (case_insensitive)
                            col_name = Poco::toLower(col_name);
                        if (!split_result.filters.contains(col_name))
                            split_result.filters.emplace(col_name, result.value().second);
                        else
                        {
                            auto merged = split_result.filters[col_name]->merge(result.value().second.get());
                            if (merged)
                                split_result.filters[col_name] = merged;
                            else
                                // doesn't support merge, use common expression push down
                                return false;
                        }
                    }
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

void pushFilterToParquetReader(const ActionsDAG & filter_expression, ParquetReader & reader)
{
    if (filter_expression.getOutputs().empty())
        return;
    auto split_result = ColumnFilterHelper::splitFilterForPushDown(std::move(filter_expression));
    for (const auto & item : split_result.filters)
    {
        reader.addFilter(item.first, item.second);
    }
    for (auto & expression_filter : split_result.expression_filters)
    {
        reader.addExpressionFilter(expression_filter);
    }
}
}
