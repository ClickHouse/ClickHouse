#pragma once
#include <Functions/IFunction.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{
/// Implements the function isNull which returns true if a value
/// is null, false otherwise.
class FunctionIsNull final : public IFunction
{
public:
    static constexpr auto name = "isNull";

    static FunctionPtr create(ContextPtr context);

    [[nodiscard]] std::string getName() const override { return name; }
    [[nodiscard]] String getSignatureString() const override { return "(Any) -> UInt8"; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    [[nodiscard]] ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
    [[nodiscard]] ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override;

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override;

#if USE_EMBEDDED_COMPILER
    [[nodiscard]] bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr &) const override { return canBeNativeType(arguments[0]); }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override;
#endif
};

}
