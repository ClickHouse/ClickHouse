#include "ColumnFilterHelper.h"
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilterFactory.h>
#include <Poco/String.h>

namespace DB
{
FilterSplitResultPtr ColumnFilterHelper::splitFilterForPushDown(const bool case_insensitive) const
{
    FilterSplitResultPtr split_result = std::make_shared<FilterSplitResult>();
    split_result->filter_expression = filter_expression.clone();
    if (split_result->filter_expression.getOutputs().empty())
        return {};
    const auto * filter_node = split_result->filter_expression.getOutputs().front();
    auto conditions = ActionsDAG::extractConjunctionAtoms(filter_node);
    std::vector<ColumnFilterPtr> filters;
    ActionsDAG::NodeRawConstPtrs unsupported_conditions;
    const auto & factories = ColumnFilterFactory::allFactories();
    for (auto condition : conditions)
    {
        // convert expr to column filter, and try to merge with existing filter on same column
        if (std::none_of(
                factories.begin(),
                factories.end(),
                [&](const ColumnFilterFactoryPtr & factory)
                {
                    // Bypass indexHint in the filter as it does not select a specific index.
                    if (condition->function_base->getName() == "indexHint")
                        return true;
                    if (!factory->validate(*condition))
                        return false;
                    NamedColumnFilter named_filter;
                    try
                    {
                        named_filter = factory->create(*condition);
                    }
                    catch (Exception &)
                    {
                        // can't convert expr to column filter
                        return false;
                    }
                    auto col_name = named_filter.first;
                    if (case_insensitive)
                        col_name = Poco::toLower(col_name);
                    if (!split_result->filters.contains(col_name))
                        split_result->filters.emplace(col_name, named_filter.second);
                    else
                    {
                        if (auto merged = split_result->filters[col_name]->merge(named_filter.second.get()))
                            split_result->filters[col_name] = merged;
                        else
                            // doesn't support merge, use common expression push down
                            return false;
                    }
                    split_result->fallback_filters[col_name].push_back(condition);
                    return true;
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

void ColumnFilterHelper::pushDownToReader(ParquetReader & reader, bool case_insensitive) const
{
    if (filter_expression.getOutputs().empty())
        return;
    auto split_result = this->splitFilterForPushDown(case_insensitive);
    reader.pushDownFilter(split_result);
}
}
