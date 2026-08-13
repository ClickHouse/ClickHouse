#include <Functions/indexHint.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

REGISTER_FUNCTION(IndexHint)
{
    FunctionDocumentation::Description description = R"(
This function is intended for debugging and introspection.
It ignores its argument and always returns 1.
The arguments are not evaluated.

During index analysis, the argument of this function is assumed to not be wrapped in `indexHint`.
This allows you to select data in index ranges by the corresponding condition but without further filtering by this condition.
The index in ClickHouse is sparse and using `indexHint` will yield more data than specifying the same condition directly.

<details>
<summary>Explanation</summary>

When you run:

```sql
SELECT * FROM test WHERE key = 123;
```

ClickHouse does two things:

1. Uses the index to find which granules (blocks of ~8192 rows) might contain `key = 123`
2. Reads those granules and filters them row-by-row to return only rows where `key = 123`

So even if it reads 8,192 rows from disk, it only returns the 1 row that actually matches.

With `indexHint`, when you run:

```sql
SELECT * FROM test WHERE indexHint(key = 123);
```

ClickHouse does only one thing:

1. Uses the index to find which granules might contain key = 123 and returns all rows from those granules **without** filtering.

It returns all 8,192 rows, including rows where `key = 456`, `key = 789`, etc. (Everything that happened to be stored in the same granule.)
`indexHint()` is not for performance. It's for debugging and understanding how ClickHouse's index works:
- Which granules does my condition select?
- How many rows are in those granules?
- Is my index being used effectively?
</details>

Note: It is not possible to optimize a query with the `indexHint` function. The `indexHint` function does not optimize the query, as it does not provide any additional information for the query analysis. Having an expression inside the `indexHint` function is not anyhow better than without the `indexHint` function. The `indexHint` function can be used only for introspection and debugging purposes and it does not improve performance. If you see the usage of `indexHint` by anyone other than ClickHouse contributors, it is likely a mistake and you should remove it.
    )";
    FunctionDocumentation::Syntax syntax = "indexHint(expression)";
    FunctionDocumentation::Arguments arguments = {
        {"expression", "Any expression for index range selection.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` in all cases.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIndexHint>(documentation);
}

}
