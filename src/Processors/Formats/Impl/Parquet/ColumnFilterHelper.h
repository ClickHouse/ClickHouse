#pragma once
#include <Interpreters/ActionsDAG.h>

namespace DB
{
class ColumnFilter;
using ColumnFilterPtr = std::shared_ptr<ColumnFilter>;
class ExpressionFilter;
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
    explicit ColumnFilterHelper(const ActionsDAG & expression)
        : filter_expression(expression.clone())
    {
    }
    void pushDownToReader(ParquetReader & reader, bool case_insensitive = false) const;

private:
    FilterSplitResultPtr splitFilterForPushDown(bool case_insensitive = false) const;


    std::optional<ActionsDAG> filter_expression;
};

}
