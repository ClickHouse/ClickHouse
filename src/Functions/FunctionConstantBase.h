#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

/// Base class for constant functions
template<typename Derived, typename T, typename ColumnT>
class FunctionConstantBase : public IFunction
{
public:
    template <typename U>
    explicit FunctionConstantBase(const U & constant_value_, bool is_distributed_ = false)
        : constant_value(static_cast<T>(constant_value_))
        , is_distributed(is_distributed_)
    {
    }

    String getName() const override
    {
        return Derived::name;
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

    /// Some functions may return different values on different shards/replicas, so it's not constant for distributed query
    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, constant_value);
    }

private:
    const T constant_value;
    bool is_distributed;
};

}
