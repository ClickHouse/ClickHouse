#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[0]).get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be an array but it has type {}.",
                            getName(), arguments[0]->getName());

        auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
        if (arguments[0]->isNullable())
            return makeNullable(result_type);
        return result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr null_map;

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(column.get()))
        {
            null_map = nullable->getNullMapColumnPtr();
            column = nullable->getNestedColumnPtr();
        }

        if (const ColumnArray * array = checkAndGetColumn<ColumnArray>(column.get()))
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

            auto result = ColumnArray::create(std::move(res_nested), array->getOffsetsPtr());
            if (result_type->isNullable())
            {
                if (!null_map)
                    null_map = ColumnUInt8::create(input_rows_count, UInt8(0));
                return ColumnNullable::create(std::move(result), std::move(null_map));
            }
            return result;
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
