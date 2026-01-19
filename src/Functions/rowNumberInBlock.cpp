#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace
{

class FunctionRowNumberInBlock : public IFunction
{
public:
    static constexpr auto name = "rowNumberInBlock";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionRowNumberInBlock>();
    }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column = ColumnUInt64::create();
        auto & data = column->getData();
        data.resize(input_rows_count);
        iota(data.data(), input_rows_count, UInt64(0));

        return column;
    }
};

}

REGISTER_FUNCTION(RowNumberInBlock)
{
    FunctionDocumentation::Description description = R"(
For each [block](../../development/architecture.md#block) processed by `rowNumberInBlock`, returns the number of the current row.

The returned number starts from 0 for each block.
    )";
    FunctionDocumentation::Syntax syntax = "rowNumberInBlock()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the ordinal number of the row in the data block starting from `0`.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT rowNumberInBlock()
FROM
(
    SELECT *
    FROM system.numbers_mt
    LIMIT 10
) SETTINGS max_block_size = 2
        )",
        R"(
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
┌─rowNumberInBlock()─┐
│                  0 │
│                  1 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRowNumberInBlock>(documentation);
}

}
