#pragma once
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct IdentityName
{
    static constexpr auto name = "identity";
};

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

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & /*types*/, const DataTypePtr & result_type) const override
    {
        return Name::name == IdentityName::name && canBeNativeType(result_type);
    }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override
    {
        return arguments[0].value;
    }
#endif
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
