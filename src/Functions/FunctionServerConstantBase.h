#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Base class for functions which return server-level constant like version() or uptime()
template<typename T, typename ColumnT, auto func_name>
class FunctionServerConstantBase : public IFunction
{
public:
    static constexpr auto name = func_name;

    explicit FunctionServerConstantBase(ContextPtr context, T && value_)
        : is_distributed(context->isDistributed())
        , value(std::forward<T>(value_))
    {
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<ColumnT>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    /// Function may return different values on different shareds/replicas, so it's not constant for distributed query
    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnT().createColumnConst(input_rows_count, value);
    }

private:
    bool is_distributed;
    T value;
};

}

