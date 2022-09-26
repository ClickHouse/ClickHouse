#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{
namespace
{

/// Return true if the column is nullable.
class FunctionIsNullable : public IFunction
{
public:
    static constexpr auto name = "isNullable";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsNullable>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & elem = arguments[0];
        return ColumnUInt8::create(input_rows_count, isColumnNullable(*elem.column) || elem.type->isLowCardinalityNullable());
    }
};

}

void registerFunctionIsNullable(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNullable>();
}

}

