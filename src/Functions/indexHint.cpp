#include <Functions/indexHint.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

REGISTER_FUNCTION(IndexHint)
{
    FunctionDocumentation::Description description_indexHint = R"(
The `indexHint` function is a debugging/optimization tool that helps you understand how ClickHouse's sparse indexing works
by letting you seee what data blocks ClickHouse would read for your condition and getting "extra" rows from blocks that don't match your condition.
It always returns `1` regardless of its input and never filters data.

When ClickHouse sees `indexHint(expression)`, it:
- Uses the condition to determine which index ranges to read
- Reads all data from those ranges
- Ignores the condition for actual filtering, returns more data than a normal `WHERE` clause would
)";
    FunctionDocumentation::Syntax syntax_indexHint = "SELECT * FROM table WHERE indexHint(expression)";
    FunctionDocumentation::Arguments arguments_indexHint = {
        {"expression", "Any expression for index range selection.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_indexHint = {"Returns `1` in all cases.", {"UInt8"}};
    FunctionDocumentation::Examples examples_indexHint = {
    {
        "Usage example with date filtering",
        R"(
SELECT FlightDate AS k, count() FROM ontime WHERE indexHint(k = '2017-09-15') GROUP BY k ORDER BY k ASC;
        )",
        R"(
┌──────────k─┬─count()─┐
│ 2017-09-14 │    7071 │
│ 2017-09-15 │   16428 │
│ 2017-09-16 │    1077 │
│ 2017-09-30 │    8167 │
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
