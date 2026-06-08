#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/ArrayJoinAction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_IS_SPECIAL;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** arrayJoin(arr) - a special function - it can not be executed directly;
  *                     is used only to get the result type of the corresponding expression.
  */
class FunctionArrayJoin : public IFunction
{
public:
    static constexpr auto name = "arrayJoin";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArrayJoin>();
    }


    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    /** It could return many different values for single argument. */
    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & arr = getArrayJoinDataType(arguments[0]);
        if (!arr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be Array or Map", getName());
        return arr->getNestedType();

    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        throw Exception(ErrorCodes::FUNCTION_IS_SPECIAL, "Function {} must not be executed directly.", getName());
    }

    /// Because of function cannot be executed directly.
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
};


REGISTER_FUNCTION(ArrayJoin)
{
    FunctionDocumentation::Description description = R"(
The `arrayJoin` function takes a row that contains an array and unfolds it, generating multiple rows – one for each element in the array.
This is in contrast to Regular Functions in ClickHouse which map input values to output values within the same row,
and Aggregate Functions which take a group of rows and "compress" or "reduce" them into a single summary row
(or a single value within a summary row if used with `GROUP BY`).

All the values in the columns are simply copied, except the values in the column where this function is applied;
these are replaced with the corresponding array value.
)";
    FunctionDocumentation::Syntax syntax = "arrayJoin(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "An array to unfold.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a set of rows unfolded from `arr`."};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", R"(SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src)", R"(
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
        )"},
        {"arrayJoin affects all sections of the query", R"(
-- The arrayJoin function affects all sections of the query, including the WHERE section. Notice the result 2, even though the subquery returned 1 row.

SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Bobruisk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
        )", R"(
┌─impressions─┐
│           2 │
└─────────────┘
        )"},
        {"Using multiple arrayJoin functions", R"(
- A query can use multiple arrayJoin functions. In this case, the transformation is performed multiple times and the rows are multiplied.

SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
        )", R"(
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Bobruisk │ Chrome  │
│           1 │ Bobruisk │ Firefox │
└─────────────┴──────────┴─────────┘
        )"
        },
        {"Unexpected results due to optimizations", R"(
-- Using multiple arrayJoin with the same expression may not produce the expected result due to optimizations.
-- For these cases, consider modifying the repeated array expression with extra operations that do not affect join result.
- e.g. arrayJoin(arraySort(arr)), arrayJoin(arrayConcat(arr, []))

SELECT
    arrayJoin(dice) as first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) as second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] as dice
);
        )", R"(
┌─first_throw─┬─second_throw─┐
│           1 │            1 │
│           1 │            2 │
│           1 │            3 │
│           1 │            4 │
│           1 │            5 │
│           1 │            6 │
│           2 │            1 │
│           2 │            2 │
│           2 │            3 │
│           2 │            4 │
│           2 │            5 │
│           2 │            6 │
│           3 │            1 │
│           3 │            2 │
│           3 │            3 │
│           3 │            4 │
│           3 │            5 │
│           3 │            6 │
│           4 │            1 │
│           4 │            2 │
│           4 │            3 │
│           4 │            4 │
│           4 │            5 │
│           4 │            6 │
│           5 │            1 │
│           5 │            2 │
│           5 │            3 │
│           5 │            4 │
│           5 │            5 │
│           5 │            6 │
│           6 │            1 │
│           6 │            2 │
│           6 │            3 │
│           6 │            4 │
│           6 │            5 │
│           6 │            6 │
└─────────────┴──────────────┘
        )"
        },
        {"Using the ARRAY JOIN syntax", R"(
-- Note the ARRAY JOIN syntax in the `SELECT` query below, which provides broader possibilities.
-- ARRAY JOIN allows you to convert multiple arrays with the same number of elements at a time.

SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
        )", R"(
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Bobruisk │ Chrome  │
└─────────────┴──────────┴─────────┘
        )"
        },
        {"Using Tuple", R"(
-- You can also use Tuple

SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Bobruisk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
        )", R"(
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Bobruisk │ Chrome  │
└─────────────┴──────────┴─────────┘
        )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionArrayJoin>(documentation);
}

}
