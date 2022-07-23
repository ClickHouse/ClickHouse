#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{
namespace
{

/// Returns 1 if and only if the argument is constant expression.
/// This function exists for development, debugging and demonstration purposes.
class FunctionIsConstant : public IFunction
{
public:
    static constexpr auto name = "isConstant";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsConstant>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

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
        return ColumnUInt8::create(input_rows_count, isColumnConst(*elem.column));
    }
};

}

void registerFunctionIsConstant(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsConstant>();
}

}

