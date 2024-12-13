#pragma once
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

    /// Do not allow any argument to have type other than String
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & arg : arguments)
        {
            if (WhichDataType(arg).isString())
                continue;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function __actionName is internal nad should not be used directly");
        }

        return FunctionIdentityBase<ActionNameName>::getReturnTypeImpl(arguments);
    }
};

}
