#include <Functions/array/arraySort.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArraySort)
{
    factory.registerFunction<FunctionArraySort>();
    factory.registerFunction<FunctionArrayReverseSort>();

    factory.registerFunction<FunctionArrayPartialSort>({
        R"(
Returns an array of the same size as the original array where elements in range `[1..limit]`
are sorted in ascending order. Remaining elements `(limit..N]` shall contain elements in unspecified order.
[example:simple_int]
[example:simple_string]

To retain only the sorted elements use `arrayResize`:
[example:retain_sorted]

If the `func` function is specified, sorting order is determined by the result of the `func`
function applied to the elements of the array.
[example:lambda_simple]

If `func` accepts multiple arguments, the `arrayPartialSort` function is passed several arrays
that the arguments of `func` will correspond to.
[example:lambda_complex]

For more details see documentation of `arraySort`.
)",
        Documentation::Examples{
            {"simple_int", "SELECT arrayPartialSort(2, [5, 9, 1, 3])"},
            {"simple_string", "SELECT arrayPartialSort(2, ['expenses','lasso','embolism','gladly'])"},
            {"retain_sorted", "SELECT arrayResize(arrayPartialSort(2, [5, 9, 1, 3]), 2)"},
            {"lambda_simple", "SELECT arrayPartialSort((x) -> -x, 2, [5, 9, 1, 3])"},
            {"lambda_complex", "SELECT arrayPartialSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res"}},
        Documentation::Categories{"Array"}});
    factory.registerFunction<FunctionArrayPartialReverseSort>({
        R"(
Returns an array of the same size as the original array where elements in range `[1..limit]`
are sorted in descending order. Remaining elements `(limit..N]` shall contain elements in unspecified order.
[example:simple_int]
[example:simple_string]

To retain only the sorted elements use `arrayResize`:
[example:retain_sorted]

If the `func` function is specified, sorting order is determined by the result of the `func`
function applied to the elements of the array.
[example:lambda_simple]

If `func` accepts multiple arguments, the `arrayPartialSort` function is passed several arrays
that the arguments of `func` will correspond to.
[example:lambda_complex]

For more details see documentation of `arraySort`.
)",
        Documentation::Examples{
            {"simple_int", "SELECT arrayPartialReverseSort(2, [5, 9, 1, 3])"},
            {"simple_string", "SELECT arrayPartialReverseSort(2, ['expenses','lasso','embolism','gladly'])"},
            {"retain_sorted", "SELECT arrayResize(arrayPartialReverseSort(2, [5, 9, 1, 3]), 2)"},
            {"lambda_simple", "SELECT arrayPartialReverseSort((x) -> -x, 2, [5, 9, 1, 3])"},
            {"lambda_complex", "SELECT arrayPartialReverseSort((x, y) -> -y, 1, [0, 1, 2], [1, 2, 3]) as res"}},
        Documentation::Categories{"Array"}});
}

}
