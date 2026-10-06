#pragma once
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Exception.h>

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

class FunctionIdentityBase : public IFunction
{
public:
    FunctionIdentityBase(const char * name_, [[maybe_unused]] bool is_identity_)
        : function_name(name_)
#if USE_EMBEDDED_COMPILER
        , is_identity(is_identity_)
#endif
    {}

    static FunctionPtr create(ContextPtr, const char * name, bool is_identity)
    {
        return std::make_shared<FunctionIdentityBase>(name, is_identity);
    }

    String getName() const override { return function_name; }
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
        return is_identity && canBeNativeType(result_type);
    }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override
    {
        return arguments[0].value;
    }
#endif

private:
    const char * function_name;
#if USE_EMBEDDED_COMPILER
    bool is_identity;
#endif
};


/// Default-constructible identity function, used as a template argument in FunctionMapToArrayAdapter
class FunctionIdentity : public FunctionIdentityBase
{
public:
    FunctionIdentity() : FunctionIdentityBase("identity", true) {}
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIdentity>(); }
};


class FunctionActionName : public FunctionIdentityBase
{
public:
    FunctionActionName() : FunctionIdentityBase("__actionName", false) {}
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

        return FunctionIdentityBase::getReturnTypeImpl(arguments);
    }
};

}
