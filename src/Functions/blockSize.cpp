#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{
namespace
{

/** columnsSize() - get the columns size in number of rows.
  */
class FunctionBlockSize : public IFunction
{
public:
    static constexpr auto name = "blockSize";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBlockSize>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
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

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnUInt64::create(input_rows_count, input_rows_count);
    }
};

}

REGISTER_FUNCTION(BlockSize)
{
    FunctionDocumentation::Description description = R"(
In ClickHouse, queries are processed in [blocks](/development/architecture#block) (chunks).
This function returns the size (row count) of the block the function is called on.
    )";
    FunctionDocumentation::Syntax syntax = "blockSize()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of rows in the current block.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT blockSize()
FROM system.numbers LIMIT 5
        )",
        R"(
┌─blockSize()─┐
│           5 │
│           5 │
│           5 │
│           5 │
│           5 │
└─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBlockSize>(documentation);
}

}
