#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <atomic>


namespace DB
{
namespace
{

/** Incremental columns number among calls of this function. */
class FunctionBlockNumber final : public IFunction
{
private:
    mutable std::atomic<size_t> columns_number{0};

public:
    static constexpr auto name = "blockNumber";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBlockNumber>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
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

    String getSignatureString() const override
    {
        return "() -> UInt64";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t current_columns_number = columns_number.fetch_add(1, std::memory_order_relaxed);
        return ColumnUInt64::create(input_rows_count, current_columns_number);
    }
};

}

REGISTER_FUNCTION(BlockNumber)
{
    FunctionDocumentation::Description description = R"(
Returns a monotonically increasing sequence number of the [block](../../development/architecture.md#block) containing the row.
The returned block number is updated on a best-effort basis, i.e. it may not be fully accurate.
    )";
    FunctionDocumentation::Syntax syntax = "blockNumber()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Sequence number of the data block where the row is located.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
SELECT blockNumber()
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 10
) SETTINGS max_block_size = 2
            )",
            R"(
┌─blockNumber()─┐
│             7 │
│             7 │
└───────────────┘
┌─blockNumber()─┐
│             8 │
│             8 │
└───────────────┘
┌─blockNumber()─┐
│             9 │
│             9 │
└───────────────┘
┌─blockNumber()─┐
│            10 │
│            10 │
└───────────────┘
┌─blockNumber()─┐
│            11 │
│            11 │
└───────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBlockNumber>(documentation);
}

}
