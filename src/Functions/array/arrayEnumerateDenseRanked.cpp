#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayEnumerateRanked.h>


namespace DB
{

class FunctionArrayEnumerateDenseRanked : public FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateDenseRanked>
{
    using Base = FunctionArrayEnumerateRankedExtended<FunctionArrayEnumerateDenseRanked>;

public:
    static constexpr auto name = "arrayEnumerateDenseRanked";
    using Base::create;
};

REGISTER_FUNCTION(ArrayEnumerateDenseRanked)
{
    FunctionDocumentation::Description description = "Returns an array the same size as the source array, indicating where each element first appears in the source array. It allows for enumeration of a multidimensional array with the ability to specify how deep to look inside the array.";
    FunctionDocumentation::Syntax syntax = "arrayEnumerateDenseRanked(clear_depth, arr, max_array_depth)";
    FunctionDocumentation::Arguments arguments = {
        {"clear_depth", "Enumerate elements at the specified level separately. Must be less than or equal to `max_arr_depth`.", {"UInt*"}},
        {"arr", "N-dimensional array to enumerate.", {"Array(T)"}},
        {"max_array_depth", "The maximum effective depth. Must be less than or equal to the depth of `arr`.", {"UInt*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array denoting where each element first appears in the source array", {"Array"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", R"(
-- With clear_depth=1 and max_array_depth=1, the result is identical to what arrayEnumerateDense would give.

SELECT arrayEnumerateDenseRanked(1,[10, 20, 10, 30],1);
        )", "[1,2,1,3]"},
        {"Usage with a multidimensional array", R"(
-- In this example, arrayEnumerateDenseRanked is used to obtain an array indicating, for each element of the
-- multidimensional array, what its position is among elements of the same value.
-- For the first row of the passed array, [10, 10, 30, 20], the corresponding first row of the result is [1, 1, 2, 3],
-- indicating that 10 is the first number encountered in position 1 and 2, 30 the second number encountered in position 3
-- and 20 is the third number encountered in position 4.
-- For the second row, [40, 50, 10, 30], the corresponding second row of the result is [4,5,1,2], indicating that 40
-- and 50 are the fourth and fifth numbers encountered in position 1 and 2 of that row, that another 10
-- (the first encountered number) is in position 3 and 30 (the second number encountered) is in the last position.

SELECT arrayEnumerateDenseRanked(1,[[10,10,30,20],[40,50,10,30]],2);
        )", "[[1,1,2,3],[4,5,1,2]]"
      },
      {"Example with increased clear_depth", R"(
-- Changing clear_depth=2 results in the enumeration occurring separately for each row anew.

SELECT arrayEnumerateDenseRanked(2,[[10,10,30,20],[40,50,10,30]],2);
        )", "[[1, 1, 2, 3], [1, 2, 3, 4]]"
      }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayEnumerateDenseRanked>(documentation);
}

}
