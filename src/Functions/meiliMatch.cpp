#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

class FunctionMeiliMatch : public IFunction
{
public:
    static constexpr auto name = "meiliMatch";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMeiliMatch>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isVariadic() const override {
        return true;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName&, const DataTypePtr&, size_t input_rows_count) const override
    {
        return ColumnUInt8::create(input_rows_count, 1u);
    }
};

}

void registerFunctionMeiliMatch(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMeiliMatch>();
}

}
