#pragma once

#include <base/types.h>
#include <boost/iostreams/detail/access_control.hpp>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <memory>

namespace DB
{

class Memo;

class IOptimizationRule
{
public:
    virtual ~IOptimizationRule() = default;
    virtual String getName() const = 0;
    virtual bool checkPattern(GroupExpressionPtr expression, const Memo & memo) const = 0;
    virtual Promise getPromise() const = 0;
    virtual bool isTransformation() const = 0;

    std::vector<GroupExpressionPtr> apply(GroupExpressionPtr expression, Memo & memo) const;

protected:
    virtual std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, Memo & memo) const = 0;
};

using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

#if 0
/// NOTE: Currently unused, replaced by JOIN graph
class JoinAssociativity : public IOptimizationRule
{
public:
    String getName() const override { return "JoinAssociativity"; }
    bool checkPattern(GroupExpressionPtr expression, const Memo & memo) const override;
    Promise getPromise() const override { return 1000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, Memo & memo) const override;
};
#endif

class JoinCommutativity : public IOptimizationRule
{
public:
    String getName() const override { return "JoinCommutativity"; }
    bool checkPattern(GroupExpressionPtr expression, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, Memo & memo) const override;
};

class HashJoinImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "HashJoin"; }
    bool checkPattern(GroupExpressionPtr expression, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, Memo & memo) const override;
};

/// Moves the QueryPlan node to implementation as is
class DefaultImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "DefaultImplementation"; }
    bool checkPattern(GroupExpressionPtr expression, const Memo & memo) const override;
    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, Memo & memo) const override;
};

}
