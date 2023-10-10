#include <Functions/isNotDistinctFrom.h>


namespace DB
{

REGISTER_FUNCTION(IsNotDistinctFrom)
{
    factory.registerFunction<FunctionIsNotDistinctFrom>(
        FunctionDocumentation{
        .description = R"(
Performs a null-safe comparison between two values. This function will consider
two `NULL` values as identical and will return `true`, which is distinct from the usual
equals behavior where comparing two `NULL` values would return `NULL`.

Currently, this function can only be used in the `JOIN ON` section of a query.
[example:join_on_is_not_distinct_from]
)",
        .examples{
            {"join_on_is_not_distinct_from", "SELECT * FROM (SELECT NULL AS a) AS t1 JOIN (SELECT NULL AS b) AS t2 ON isNotDistinctFrom(t1.a, t2.b)", "NULL\tNULL"},
        },
        .categories = {"Comparison", "Join Operators"},
    });

}

}
