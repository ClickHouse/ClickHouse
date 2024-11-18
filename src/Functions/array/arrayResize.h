#pragma once
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class FunctionArrayResize : public IFunction
{
public:
    static constexpr auto name = "arrayResize";
    static FunctionPtr createImpl() { return std::make_shared<FunctionArrayResize>(); }
    static FunctionPtr create(ContextPtr) { return createImpl(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override;
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
};

}
