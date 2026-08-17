#include <Functions/array/arrayEnumerateExtended.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionArrayEnumerateUniq : public FunctionArrayEnumerateExtended<FunctionArrayEnumerateUniq>
{
    using Base = FunctionArrayEnumerateExtended<FunctionArrayEnumerateUniq>;
public:
    static constexpr auto name = "arrayEnumerateUniq";
    using Base::create;
};

REGISTER_FUNCTION(ArrayEnumerateUniq)
{
    FunctionDocumentation::Description description = R"(
Returns an array the same size as the source array, indicating for each element what its position is among elements with the same value.

This function is useful when using `ARRAY JOIN` and aggregation of array elements.

The function can take multiple arrays of the same size as arguments. In this case, uniqueness is considered for tuples of elements in the same positions in all the arrays.
)";
    FunctionDocumentation::Syntax syntax = "arrayEnumerateUniq(arr1[, arr2, ... , arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"arr1", "First array to process.", {"Array(T)"}},
        {"arr2, ...", "Optional. Additional arrays of the same size for tuple uniqueness.", {"Array(UInt32)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array where each element is the position among elements with the same value or tuple.", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT arrayEnumerateUniq([10, 20, 10, 30]);", "[1, 1, 2, 1]"},
        {"Multiple arrays", "SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]);", "[1,2,1,1,2,1]"},
        {"ARRAY JOIN aggregation",
         R"(
-- Each goal ID has a calculation of the number of conversions (each element in the Goals nested data structure is a goal that was reached, which we refer to as a conversion)
-- and the number of sessions. Without ARRAY JOIN, we would have counted the number of sessions as sum(Sign). But in this particular case,
-- the rows were multiplied by the nested Goals structure, so in order to count each session one time after this, we apply a condition to the
-- value of the arrayEnumerateUniq(Goals.ID) function.

SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
)",
R"(
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayEnumerateUniq>(documentation);
}

}
