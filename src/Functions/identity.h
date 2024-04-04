#pragma once
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

template<typename Name>
class FunctionIdentityBase : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIdentityBase<Name>>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments.front();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        return arguments.front().column;
    }
};

struct IdentityName
{
    static constexpr auto name = "identity";
};

struct ScalarSubqueryResultName
{
    static constexpr auto name = "__scalarSubqueryResult";
};

using FunctionIdentity = FunctionIdentityBase<IdentityName>;
using FunctionScalarSubqueryResult = FunctionIdentityBase<ScalarSubqueryResultName>;

struct ActionNameName
{
    static constexpr auto name = "__actionName";
};

class FunctionActionName : public FunctionIdentityBase<ActionNameName>
{
public:
    using FunctionIdentityBase::FunctionIdentityBase;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionActionName>(); }
    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }
};

}
