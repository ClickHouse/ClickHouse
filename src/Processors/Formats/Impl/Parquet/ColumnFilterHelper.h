#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>

namespace DB
{
using ColumnFilterCreator = std::function<OptionalFilter(const ActionsDAG::Node &)>;
using ColumnFilterCreators = std::vector<ColumnFilterCreator>;

class ParquetReader;
struct FilterSplitResult
{
    ActionsDAG filter_expression;
    std::unordered_map<String, ColumnFilterPtr> filters;
    std::unordered_map<String, ActionsDAG::NodeRawConstPtrs> fallback_filters;
    std::vector<std::shared_ptr<ExpressionFilter>> expression_filters;
};

using FilterSplitResultPtr = std::shared_ptr<FilterSplitResult>;

class ColumnFilterHelper
{
public:
    static FilterSplitResultPtr splitFilterForPushDown(const ActionsDAG & filter_expression, bool case_insensitive = false);

private:
    static ColumnFilterCreators creators;
};

void pushFilterToParquetReader(const ActionsDAG & filter_expression, ParquetReader & reader);
}
