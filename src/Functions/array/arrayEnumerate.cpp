#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/// arrayEnumerate(arr) - Returns the array [1,2,3,..., length(arr)]
class FunctionArrayEnumerate final : public IFunction
{
public:
    static constexpr auto name = "arrayEnumerate";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArrayEnumerate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    String getSignatureString() const override { return "(Array) -> Array(UInt32)"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        if (const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get()))
        {
            const ColumnArray::Offsets & offsets = array->getOffsets();

            auto res_nested = ColumnUInt32::create();

            ColumnUInt32::Container & res_values = res_nested->getData();
            res_values.resize(array->getData().size());
            ColumnArray::Offset prev_off = 0;
            for (auto off : offsets)
            {
                for (ColumnArray::Offset j = prev_off; j < off; ++j)
                    res_values[j] = static_cast<UInt32>(j - prev_off + 1);
                prev_off = off;
            }

            return ColumnArray::create(std::move(res_nested), array->getOffsetsPtr());
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};


REGISTER_FUNCTION(ArrayEnumerate)
{
    FunctionDocumentation::Description description = R"(
Returns the array `[1, 2, 3, ..., length (arr)]`

This function is normally used with the [`ARRAY JOIN`](/sql-reference/statements/select/array-join) clause. It allows counting something just
once for each array after applying `ARRAY JOIN`.
This function can also be used in higher-order functions. For example, you can use it to get array indexes for elements that match a condition.
)";
    FunctionDocumentation::Syntax syntax = "arrayEnumerate(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array to enumerate.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the array `[1, 2, 3, ..., length (arr)]`.", {"Array(UInt32)"}};
    FunctionDocumentation::Examples examples = {{"Basic example with ARRAY JOIN", R"(
CREATE TABLE test
(
    `id` UInt8,
    `tag` Array(String),
    `version` Array(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, ['release-stable', 'dev', 'security'], ['2.4.0', '2.6.0-alpha', '2.4.0-sec1']);

SELECT
    id,
    tag,
    version,
    seq
FROM test
ARRAY JOIN
    tag,
    version,
    arrayEnumerate(tag) AS seq
    )", R"(
┌─id─┬─tag────────────┬─version─────┬─seq─┐
│  1 │ release-stable │ 2.4.0       │   1 │
│  1 │ dev            │ 2.6.0-alpha │   2 │
│  1 │ security       │ 2.4.0-sec1  │   3 │
└────┴────────────────┴─────────────┴─────┘
    )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayEnumerate>(documentation);
}

}
