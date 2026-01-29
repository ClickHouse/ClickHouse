#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace
{

/** ignore(...) is a function that takes any arguments, and always returns 0.
  */
class FunctionIgnore : public IFunction
{
public:
    static constexpr auto name = "ignore";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIgnore>();
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// We should never return LowCardinality result, cause we declare that result is always constant zero.
    /// (in getResultIfAlwaysReturnsConstantAndHasArguments)
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool useDefaultImplementationForSparseColumns() const override { return false; }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeUInt8().createColumnConst(input_rows_count, 0u);
    }
};

}

REGISTER_FUNCTION(Ignore)
{
    FunctionDocumentation::Description description = R"(
Accepts arbitrary arguments and unconditionally returns `0`.
    )";
    FunctionDocumentation::Syntax syntax = "ignore(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "An input value which is unused and passed only so as to avoid a syntax error.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Always returns `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT ignore(0, 'ClickHouse', NULL)
        )",
        R"(
┌─ignore(0, 'ClickHouse', NULL)─┐
│                             0 │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIgnore>(documentation);
}

}
