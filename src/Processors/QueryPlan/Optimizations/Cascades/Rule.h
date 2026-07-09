#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <memory>

namespace DB
{

class Memo;
class IQueryPlanStep;

/// A Full sort with a limit is a top-N: it reduces rows, so it stays in the memo as an operator
/// (a limit-less Full sort is stripped into a sorting property). Only a Full sort takes unsorted
/// input; FinishSorting/MergingSorted need ordered input, which no rule provides.
bool isTopNSort(const IQueryPlanStep & step);

class IOptimizationRule
{
public:
    virtual ~IOptimizationRule() = default;
    virtual String getName() const = 0;
    virtual bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const = 0;
    virtual Promise getPromise() const = 0;
    virtual bool isTransformation() const = 0;

    std::vector<GroupExpressionPtr> apply(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const;

protected:
    virtual std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const = 0;
};

using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

}
