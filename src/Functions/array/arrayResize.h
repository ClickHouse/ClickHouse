#pragma once
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class FunctionArrayResize final : public IFunction
{
public:
    static constexpr auto name = "arrayResize";
    static FunctionPtr createImpl() { return std::make_shared<FunctionArrayResize>(); }
    static FunctionPtr create(ContextPtr) { return createImpl(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override;
};

}
