#include "ColumnFilterHelper.h"
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
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

FilterSplitResultPtr ColumnFilterHelper::splitFilterForPushDown(const ActionsDAG & filter_expression, bool case_insensitive)
{
    FilterSplitResultPtr split_result = std::make_shared<FilterSplitResult>();
    split_result->filter_expression = filter_expression.clone();
    if (split_result->filter_expression.getOutputs().empty())
        return {};
    const auto * filter_node = split_result->filter_expression.getOutputs().front();
    auto conditions = ActionsDAG::extractConjunctionAtoms(filter_node);
    std::vector<ColumnFilterPtr> filters;
    ActionsDAG::NodeRawConstPtrs unsupported_conditions;
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
                        if (!split_result->filters.contains(col_name))
                            split_result->filters.emplace(col_name, result.value().second);
                        else
                        {
                            auto merged = split_result->filters[col_name]->merge(result.value().second.get());
                            if (merged)
                                split_result->filters[col_name] = merged;
                            else
                                // doesn't support merge, use common expression push down
                                return false;
                        }
                        split_result->fallback_filters[col_name].push_back(condition);
                    }
                    return result.has_value();
                }))
            unsupported_conditions.push_back(condition);
    }
    auto actions_dag = ActionsDAG::buildFilterActionsDAG(unsupported_conditions);
    if (actions_dag.has_value())
    {
        split_result->expression_filters.emplace_back(std::make_shared<ExpressionFilter>(std::move(actions_dag.value())));
    }
    return split_result;
}

void pushFilterToParquetReader(const ActionsDAG & filter_expression, ParquetReader & reader)
{
    if (filter_expression.getOutputs().empty())
        return;
    auto split_result = ColumnFilterHelper::splitFilterForPushDown(filter_expression);
    reader.pushDownFilter(split_result);
}
}
