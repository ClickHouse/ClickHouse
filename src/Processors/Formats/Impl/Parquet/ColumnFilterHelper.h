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
    std::vector<std::shared_ptr<ExpressionFilter>> expression_filters;
};

class ColumnFilterHelper
{
public:

    static FilterSplitResult splitFilterForPushDown(const ActionsDAG& filter_expression);

private:
    static ColumnFilterCreators creators;
};

void pushFilterToParquetReader(const ActionsDAG& filter_expression, ParquetReader & reader);
}




