#include <Functions/indexHint.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

REGISTER_FUNCTION(IndexHint)
{
    FunctionDocumentation::Description description_indexHint = R"(
This function is intended for debugging and introspection.
It ignores its argument and always returns 1.
The arguments are not evaluated.

But during index analysis, the argument of this function is assumed to be not wrapped in `indexHint`.
This allows to select data in index ranges by the corresponding condition but without further filtering by this condition.
The index in ClickHouse is sparse and using `indexHint` will yield more data than specifying the same condition directly.
    )";
    FunctionDocumentation::Syntax syntax_indexHint = "indexHint(expression)";
    FunctionDocumentation::Arguments arguments_indexHint = {
        {"expression", "Any expression for index range selection.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_indexHint = {"Returns `1` in all cases.", {"UInt8"}};
    FunctionDocumentation::Examples examples_indexHint = {
    {
        "Usage example with date filtering",
        R"(
SELECT FlightDate AS k, count() FROM ontime WHERE indexHint(k = '2025-09-15') GROUP BY k ORDER BY k ASC;
        )",
        R"(
┌──────────k─┬─count()─┐
│ 2025-09-14 │    7071 │
│ 2025-09-15 │   16428 │
│ 2025-09-16 │    1077 │
│ 2025-09-30 │    8167 │
└────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_indexHint = {1, 1};
    FunctionDocumentation::Category category_indexHint = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_indexHint = {description_indexHint, syntax_indexHint, arguments_indexHint, returned_value_indexHint, examples_indexHint, introduced_in_indexHint, category_indexHint};

    factory.registerFunction<FunctionIndexHint>(documentation_indexHint);
}

}
