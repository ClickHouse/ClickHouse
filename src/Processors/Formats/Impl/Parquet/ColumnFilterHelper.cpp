#include "ColumnFilterHelper.h"

namespace DB
{

ColumnFilterCreators ColumnFilterHelper::creators = {BigIntRangeFilter::create, createFloatRangeFilter, ByteValuesFilter::create, NegatedBigIntRangeFilter::create};
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
    if (split_result.remain_filter.has_value())
        reader.setRemainFilter(split_result.remain_filter);
}
}
