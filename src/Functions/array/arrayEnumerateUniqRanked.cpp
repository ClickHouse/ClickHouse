#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayEnumerateRanked.h>


namespace DB
{

class FunctionArrayEnumerateUniqRanked : public FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateUniqRanked>
{
    using Base = FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateUniqRanked>;

public:
    static constexpr auto name = "arrayEnumerateUniqRanked";
    using Base::create;
};

REGISTER_FUNCTION(ArrayEnumerateUniqRanked)
{
    FunctionDocumentation::Description description = R"(
Returns an array (or multi-dimensional array) with the same dimensions as the source array,
indicating for each element what it's position is among elements with the same value.
It allows for enumeration of a multi-dimensional array with the ability to specify how deep to look inside the array.
)";
    FunctionDocumentation::Syntax syntax = "arrayEnumerateUniqRanked(clear_depth, arr, max_array_depth)";
    FunctionDocumentation::Arguments arguments = {
        {"clear_depth", "Enumerate elements at the specified level separately. Positive integer less than or equal to `max_arr_depth`.", {"UInt*"}},
        {"arr", "N-dimensional array to enumerate.", {"Array(T)"}},
        {"max_array_depth", "The maximum effective depth. Positive integer less than or equal to the depth of `arr`.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an N-dimensional array the same size as `arr` with each element showing the position of that element in relation to other elements of the same value.", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Example 1", R"(
-- With clear_depth=1 and max_array_depth=1, the result of arrayEnumerateUniqRanked
-- is identical to that which arrayEnumerateUniq would give for the same array.

SELECT arrayEnumerateUniqRanked(1, [1, 2, 1], 1);
        )", "[1, 1, 2]"},
        {"Example 2", R"(
-- with clear_depth=1 and max_array_depth=1, the result of arrayEnumerateUniqRanked
-- is identical to that which arrayEnumerateUniqwould give for the same array.

SELECT arrayEnumerateUniqRanked(1, [[1, 2, 3], [2, 2, 1], [3]], 2);", "[[1, 1, 1], [2, 3, 2], [2]]

)", "[1, 1, 2]"},
        {"Example 3", R"(
-- In this example, arrayEnumerateUniqRanked is used to obtain an array indicating,
-- for each element of the multidimensional array, what its position is among elements
-- of the same value. For the first row of the passed array, [1, 2, 3], the corresponding
-- result is [1, 1, 1], indicating that this is the first time 1, 2 and 3 are encountered.
-- For the second row of the provided array, [2, 2, 1], the corresponding result is [2, 3, 3],
-- indicating that 2 is encountered for a second and third time, and 1 is encountered
-- for the second time. Likewise, for the third row of the provided array [3] the
-- corresponding result is [2] indicating that 3 is encountered for the second time.

SELECT arrayEnumerateUniqRanked(1, [[1, 2, 3], [2, 2, 1], [3]], 2);
        )", "[[1, 1, 1], [2, 3, 2], [2]]"
    },
    {"Example 4", R"(
-- Changing clear_depth=2, results in elements being enumerated separately for each row.
SELECT arrayEnumerateUniqRanked(2,[[1, 2, 3],[2, 2, 1],[3]], 2);
        )", "[[1, 1, 1], [1, 2, 1], [1]]"},
};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayEnumerateUniqRanked>(documentation);
}

}
