#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(RunningDifference)
{
    FunctionDocumentation::Description description = R"(
Calculates the difference between two consecutive row values in the data block.
Returns `0` for the first row, and for subsequent rows the difference to the previous row.

:::warning Deprecated
Only returns differences inside the currently processed data block.
Because of this error-prone behavior, the function is deprecated.
It is advised to use [window functions](/sql-reference/window-functions) instead.

You can use setting [`allow_deprecated_error_prone_window_functions`](/operations/settings/settings#allow_deprecated_error_prone_window_functions) to allow usage of this function.
:::

The result of the function depends on the affected data blocks and the order of data in the block.
The order of rows during calculation of `runningDifference()` can differ from the order of rows returned to the user.
To prevent that you can create a subquery with [`ORDER BY`](../../sql-reference/statements/select/order-by.md) and call the function from outside the subquery.
Please note that the block size affects the result.
The internal state of `runningDifference` state is reset for each new block.
)";
    FunctionDocumentation::Syntax syntax = "runningDifference(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "Column for which to calculate the running difference.", {"Any"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the difference between consecutive values, with 0 for the first row.", {}};
    FunctionDocumentation::Examples examples = {{"Usage example",
        R"(
SET allow_deprecated_error_prone_window_functions = 1;

CREATE TABLE events
(
    EventID UInt32,
    EventDate Date,
    EventTime DateTime
)
ENGINE = Memory;

INSERT INTO events VALUES
    (1106, '2025-11-24', '2025-11-24 00:00:04'),
    (1107, '2025-11-24', '2025-11-24 00:00:05'),
    (1108, '2025-11-24', '2025-11-24 00:00:05'),
    (1109, '2025-11-24', '2025-11-24 00:00:09'),
    (1110, '2025-11-24', '2025-11-24 00:00:10');

SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2025-11-24'
    ORDER BY EventTime ASC, EventID ASC
    LIMIT 5
);
        )",
        R"(
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2025-11-24 00:00:04 │     0 │
│    1107 │ 2025-11-24 00:00:05 │     1 │
│    1108 │ 2025-11-24 00:00:05 │     0 │
│    1109 │ 2025-11-24 00:00:09 │     4 │
│    1110 │ 2025-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
        )"
    },
    {
        "Block size impact example",
        R"(
SET allow_deprecated_error_prone_window_functions = 1;
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1;
        )",
        R"(
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRunningDifferenceImpl<true>>(documentation);
}

}
