#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Implements the function assumeNotNull which takes 1 argument and works as follows:
/// - if the argument is a nullable column, return its embedded column;
/// - otherwise return the original argument.
/// NOTE: assumeNotNull may not be called with the NULL value.
class FunctionAssumeNotNull : public IFunction
{
public:
    static constexpr auto name = "assumeNotNull";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionAssumeNotNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return removeNullable(arguments[0]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnPtr & col = arguments[0].column;

        if (arguments[0].type->onlyNull() && !col->empty())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot create non-empty column with type Nothing");

        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(&*col))
            return nullable_col->getNestedColumnPtr();
        return col;
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr &) const override { return canBeNativeType(arguments[0]); }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        if (arguments[0].type->isNullable())
            return b.CreateExtractValue(arguments[0].value, {0});
        else
            return arguments[0].value;
    }
#endif


};

}

REGISTER_FUNCTION(AssumeNotNull)
{
    factory.registerFunction<FunctionAssumeNotNull>();
}

}
