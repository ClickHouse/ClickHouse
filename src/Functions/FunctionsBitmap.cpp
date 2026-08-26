#include <Functions/FunctionFactory.h>

/// TODO include this last because of a broken roaring header. See the comment inside.
#include <Functions/FunctionsBitmap.h>


namespace DB
{

REGISTER_FUNCTION(Bitmap)
{
    /// Documentation for bitmapBuild
    FunctionDocumentation::Description description_bitmapBuild = "Builds a bitmap from an integer array. Supported element types are signed and unsigned integers of 8, 16, 32, or 64 bits. It is the opposite of function [`bitmapToArray`](/reference/functions/regular-functions/bitmap-functions#bitmapToArray).";
    FunctionDocumentation::Syntax syntax_bitmapBuild = "bitmapBuild(array)";
    FunctionDocumentation::Arguments arguments_bitmapBuild = {
        {"array", "Integer array.", {"Array((U)Int*)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapBuild = {"Returns a bitmap from the provided array", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapBuild = {
        {"Usage example", R"(
-- A bitmap is a binary value, so it is shown with `hex`.
SELECT hex(bitmapBuild([1, 2, 3, 4, 5])) AS res, toTypeName(bitmapBuild([1, 2, 3, 4, 5])) AS type;
        )",
        R"(
┌─res────────────┬─type──────────────────────────────────┐
│ 00050102030405 │ AggregateFunction(groupBitmap, UInt8) │
└────────────────┴───────────────────────────────────────┘
        )"},
        {"Signed bitmap", R"(
-- A bitmap is a binary value, so it is shown with `hex`.
SELECT hex(bitmapBuild([-128, -1]::Array(Int8))) AS res, toTypeName(bitmapBuild([-128, -1]::Array(Int8))) AS type;
        )",
        R"(
┌─res──────┬─type─────────────────────────────────┐
│ 000280FF │ AggregateFunction(groupBitmap, Int8) │
└──────────┴──────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapBuild = {20, 1};
    FunctionDocumentation::Category category_bitmapBuild = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapBuild = {description_bitmapBuild, syntax_bitmapBuild, arguments_bitmapBuild, {}, returned_value_bitmapBuild, examples_bitmapBuild, introduced_in_bitmapBuild, category_bitmapBuild};

    factory.registerFunction<FunctionBitmapBuild>(documentation_bitmapBuild);

    /// Documentation for bitmapToArray
    FunctionDocumentation::Description description_bitmapToArray = "Converts a bitmap to an array of its elements. The array element type matches the bitmap element type `T` (signed or unsigned integer). It is the opposite of function [`bitmapBuild`](/reference/functions/regular-functions/bitmap-functions#bitmapBuild).";
    FunctionDocumentation::Syntax syntax_bitmapToArray = "bitmapToArray(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapToArray = {
        {"bitmap", "Bitmap to convert. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapToArray = {"Returns an array of the elements contained in the bitmap", {"Array(T)"}};
    FunctionDocumentation::Examples examples_bitmapToArray = {
        {"Usage example", "SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;",
        R"(
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
        )"},
        {"Signed bitmap", "SELECT arraySort(bitmapToArray(bitmapBuild([-128, -1]::Array(Int8)))) AS res;",
        R"(
┌─res───────┐
│ [-128,-1] │
└───────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapToArray = {20, 1};
    FunctionDocumentation::Category category_bitmapToArray = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapToArray = {description_bitmapToArray, syntax_bitmapToArray, arguments_bitmapToArray, {}, returned_value_bitmapToArray, examples_bitmapToArray, introduced_in_bitmapToArray, category_bitmapToArray};

    factory.registerFunction<FunctionBitmapToArray>(documentation_bitmapToArray);

    /// Documentation for bitmapSubsetInRange
    FunctionDocumentation::Description description_bitmapSubsetInRange = "Returns a subset of the bitmap containing elements in the value range `[start, end)`. Element values are compared as unsigned integers of the bitmap element type.";
    FunctionDocumentation::Syntax syntax_bitmapSubsetInRange = "bitmapSubsetInRange(bitmap, start, end)";
    FunctionDocumentation::Arguments arguments_bitmapSubsetInRange = {
        {"bitmap", "Bitmap to extract the subset from. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"start", "Start of the range (inclusive). [`UInt*`](/reference/data-types/int-uint)"},
        {"end", "End of the range (exclusive). [`UInt*`](/reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapSubsetInRange = {"Returns a bitmap containing only the elements in the specified value range", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapSubsetInRange = {{"Usage example", "SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([1, 2, 3, 4, 5]), 2, 5)) AS res;",
        R"(
┌─res─────┐
│ [2,3,4] │
└─────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapSubsetInRange = {20, 1};
    FunctionDocumentation::Category category_bitmapSubsetInRange = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapSubsetInRange = {description_bitmapSubsetInRange, syntax_bitmapSubsetInRange, arguments_bitmapSubsetInRange, {}, returned_value_bitmapSubsetInRange, examples_bitmapSubsetInRange, introduced_in_bitmapSubsetInRange, category_bitmapSubsetInRange};

    factory.registerFunction<FunctionBitmapSubsetInRange>(documentation_bitmapSubsetInRange);

    /// Documentation for bitmapSubsetLimit
    FunctionDocumentation::Description description_bitmapSubsetLimit = "Returns a subset of at most `cardinality_limit` elements whose values are greater than or equal to `range_start`, selecting the smallest such values in unsigned order.";
    FunctionDocumentation::Syntax syntax_bitmapSubsetLimit = "bitmapSubsetLimit(bitmap, range_start, cardinality_limit)";
    FunctionDocumentation::Arguments arguments_bitmapSubsetLimit = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"range_start", "Start of the range (inclusive). [`UInt32`](/reference/data-types/int-uint)"},
        {"cardinality_limit", "Maximum cardinality of the subset. [`UInt32`](/reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapSubsetLimit = {"Returns a bitmap containing at most `cardinality_limit` elements with unsigned value at least `range_start`", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapSubsetLimit = {{"Usage example", "SELECT arraySort(bitmapToArray(bitmapSubsetLimit(bitmapBuild([1, 5, 3, 2, 8]), 3, 2))) AS res;",
        R"(
┌─res───┐
│ [3,5] │
└───────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapSubsetLimit = {20, 1};
    FunctionDocumentation::Category category_bitmapSubsetLimit = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapSubsetLimit = {description_bitmapSubsetLimit, syntax_bitmapSubsetLimit, arguments_bitmapSubsetLimit, {}, returned_value_bitmapSubsetLimit, examples_bitmapSubsetLimit, introduced_in_bitmapSubsetLimit, category_bitmapSubsetLimit};

    factory.registerFunction<FunctionBitmapSubsetLimit>(documentation_bitmapSubsetLimit);

    /// Documentation for subBitmap (bitmapSubsetOffsetLimit is the unregistered alias see: https://github.com/ClickHouse/ClickHouse/pull/27234)
    FunctionDocumentation::Description description_subBitmap = "Returns a subset of the bitmap after skipping `offset` elements in ascending unsigned value order. The maximum cardinality of the returned bitmap is `cardinality_limit`.";
    FunctionDocumentation::Syntax syntax_subBitmap = "subBitmap(bitmap, offset, cardinality_limit)";
    FunctionDocumentation::Arguments arguments_subBitmap = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"offset", "Number of set bits to skip from the beginning (zero-based). [`UInt32`](/reference/data-types/int-uint)"},
        {"cardinality_limit", "Maximum number of set bits to include in the subset. [`UInt32`](/reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_subBitmap = {"Returns a bitmap containing at most `cardinality_limit` elements after skipping `offset` elements in ascending unsigned value order", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_subBitmap = {{"Usage example", "SELECT bitmapToArray(subBitmap(bitmapBuild([1, 2, 3, 4, 5]), 2, 2)) AS res;",
        R"(
┌─res───┐
│ [3,4] │
└───────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subBitmap = {21, 9};
    FunctionDocumentation::Category category_subBitmap = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_subBitmap = {description_subBitmap, syntax_subBitmap, arguments_subBitmap, {}, returned_value_subBitmap, examples_subBitmap, introduced_in_subBitmap, category_subBitmap};

    factory.registerFunction<FunctionBitmapSubsetOffsetLimit>(documentation_subBitmap);

    /// Documentation for bitmapTransform
    FunctionDocumentation::Description description_bitmapTransform = R"(
Replaces elements in a bitmap according to a mapping from `from_array` to `to_array`.
Values are interpreted as unsigned integers of the bitmap element type (same domain as [`bitmapContains`](/sql-reference/functions/bitmap-functions#bitmapContains)).
For signed bitmaps, a negative element matches its unsigned counterpart (for example, `Int8` value `-1` matches `255`).
A value in `to_array` that does not fit into the bitmap element type raises `BAD_ARGUMENTS`. A value in `from_array`
that does not fit is simply not found, so the corresponding replacement does not apply.
    )";
    FunctionDocumentation::Syntax syntax_bitmapTransform = "bitmapTransform(bitmap, from_array, to_array)";
    FunctionDocumentation::Arguments arguments_bitmapTransform =
    {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"from_array", "Array of original set bits to be replaced. [`Array(T)`](/reference/data-types/array)."},
        {"to_array", "Array of new set bits to replace with. [`Array(T)`](/reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapTransform = {"Returns a bitmap with elements transformed according to the given mapping", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapTransform =
        {{"Usage example", "SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5]), [2, 4], [20, 40])) AS res;",
        R"(
┌─res───────────┐
│ [1,3,5,20,40] │
└───────────────┘
    )"},
        {"Signed bitmap", "SELECT arraySort(bitmapToArray(bitmapTransform(bitmapBuild([-1, 0]::Array(Int8)), [255], [10]))) AS res;",
        R"(
┌─res────┐
│ [0,10] │
└────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapTransform = {20, 1};
    FunctionDocumentation::Category category_bitmapTransform = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapTransform = {description_bitmapTransform, syntax_bitmapTransform, arguments_bitmapTransform, {}, returned_value_bitmapTransform, examples_bitmapTransform, introduced_in_bitmapTransform, category_bitmapTransform};

    factory.registerFunction<FunctionBitmapTransform>(documentation_bitmapTransform);

    /// Documentation for bitmapCardinality
    FunctionDocumentation::Description description_bitmapCardinality = "Returns the number of bits set (the cardinality) in the bitmap.";
    FunctionDocumentation::Syntax syntax_bitmapCardinality = "bitmapCardinality(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapCardinality = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapCardinality = {"Returns the number of bits set in the bitmap", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapCardinality = {{"Usage example", "SELECT bitmapCardinality(bitmapBuild([1, 3, 3, 5, 7, 7])) AS res",
        R"(
┌─res─┐
│   4 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapCardinality = {description_bitmapCardinality, syntax_bitmapCardinality, arguments_bitmapCardinality, {}, returned_value_bitmapCardinality, examples_bitmapCardinality, introduced_in_bitmapCardinality, category_bitmapCardinality};

    factory.registerFunction<FunctionBitmapSelfCardinality>(documentation_bitmapCardinality);

    /// Documentation for bitmapMin
    FunctionDocumentation::Description description_bitmapMin = "Returns the smallest element in a bitmap, interpreted as an unsigned integer of the bitmap element type. For signed bitmaps, negative values are treated as their unsigned counterparts (for example, `Int8` value `-128` is `128`). If the bitmap is empty, returns `UINT32_MAX` (`UINT64_MAX` if the bitmap element type is wider than 32 bits).";
    FunctionDocumentation::Syntax syntax_bitmapMin = "bitmapMin(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapMin = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapMin = {"Returns the smallest element as an unsigned value of the bitmap element type, or `UINT32_MAX`/`UINT64_MAX` if the bitmap is empty", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapMin = {
        {"Usage example", "SELECT bitmapMin(bitmapBuild([3, 5, 2, 6])) AS res;",
        R"(
┌─res─┐
│   2 │
└─────┘
    )"},
        {"Signed bitmap", "SELECT bitmapMin(bitmapBuild([-128, -1]::Array(Int8))) AS res;",
        R"(
┌─res─┐
│ 128 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapMin = {20, 1};
    FunctionDocumentation::Category category_bitmapMin = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapMin = {description_bitmapMin, syntax_bitmapMin, arguments_bitmapMin, {}, returned_value_bitmapMin, examples_bitmapMin, introduced_in_bitmapMin, category_bitmapMin};

    factory.registerFunction<FunctionBitmapMin>(documentation_bitmapMin);

    /// Documentation for bitmapMax
    FunctionDocumentation::Description description_bitmapMax = "Returns the greatest element in a bitmap, interpreted as an unsigned integer of the bitmap element type. For signed bitmaps, negative values are treated as their unsigned counterparts (for example, `Int8` value `-1` is `255`). Returns `0` if the bitmap is empty.";
    FunctionDocumentation::Syntax syntax_bitmapMax = "bitmapMax(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapMax = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapMax = {"Returns the greatest element as an unsigned value of the bitmap element type, or `0` if the bitmap is empty", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapMax = {
        {"Usage example", "SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res;",
        R"(
┌─res─┐
│   5 │
└─────┘
    )"},
        {"Signed bitmap", "SELECT bitmapMax(bitmapBuild([-128, -1]::Array(Int8))) AS res;",
        R"(
┌─res─┐
│ 255 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapMax = {20, 1};
    FunctionDocumentation::Category category_bitmapMax = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapMax = {description_bitmapMax, syntax_bitmapMax, arguments_bitmapMax, {}, returned_value_bitmapMax, examples_bitmapMax, introduced_in_bitmapMax, category_bitmapMax};

    factory.registerFunction<FunctionBitmapMax>(documentation_bitmapMax);

    /// Documentation for bitmapAndCardinality
    FunctionDocumentation::Description description_bitmapAndCardinality = "Returns the cardinality of the logical conjunction (AND) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAndCardinality = "bitmapAndCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAndCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAndCardinality = {"Returns the number of set bits in the intersection of the two bitmaps", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapAndCardinality = {{"Usage example", "SELECT bitmapAndCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   1 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAndCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapAndCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAndCardinality = {description_bitmapAndCardinality, syntax_bitmapAndCardinality, arguments_bitmapAndCardinality, {}, returned_value_bitmapAndCardinality, examples_bitmapAndCardinality, introduced_in_bitmapAndCardinality, category_bitmapAndCardinality};

    factory.registerFunction<FunctionBitmapAndCardinality>(documentation_bitmapAndCardinality);

    /// Documentation for bitmapOrCardinality
    FunctionDocumentation::Description description_bitmapOrCardinality = "Returns the cardinality of the logical disjunction (OR) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapOrCardinality = "bitmapOrCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapOrCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapOrCardinality = {"Returns the number of set bits in the union of the two bitmaps", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapOrCardinality = {{"Usage example", "SELECT bitmapOrCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   5 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapOrCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapOrCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapOrCardinality = {description_bitmapOrCardinality, syntax_bitmapOrCardinality, arguments_bitmapOrCardinality, {}, returned_value_bitmapOrCardinality, examples_bitmapOrCardinality, introduced_in_bitmapOrCardinality, category_bitmapOrCardinality};

    factory.registerFunction<FunctionBitmapOrCardinality>(documentation_bitmapOrCardinality);

    /// Documentation for bitmapXorCardinality
    FunctionDocumentation::Description description_bitmapXorCardinality = "Returns the cardinality of the XOR (symmetric difference) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapXorCardinality = "bitmapXorCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapXorCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapXorCardinality = {"Returns the number of set bits in the symmetric difference of the two bitmaps", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapXorCardinality = {{"Usage example", "SELECT bitmapXorCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   4 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapXorCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapXorCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapXorCardinality = {description_bitmapXorCardinality, syntax_bitmapXorCardinality, arguments_bitmapXorCardinality, {}, returned_value_bitmapXorCardinality, examples_bitmapXorCardinality, introduced_in_bitmapXorCardinality, category_bitmapXorCardinality};

    factory.registerFunction<FunctionBitmapXorCardinality>(documentation_bitmapXorCardinality);

    /// Documentation for bitmapAndnotCardinality
    FunctionDocumentation::Description description_bitmapAndnotCardinality = "Returns the cardinality of the AND-NOT operation of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAndnotCardinality = "bitmapAndnotCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAndnotCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAndnotCardinality = {"Returns the number of set bits in the result of `bitmap1 AND-NOT bitmap2`", {"UInt64"}};
    FunctionDocumentation::Examples examples_bitmapAndnotCardinality = {{"Usage example", "SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   2 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAndnotCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapAndnotCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAndnotCardinality = {description_bitmapAndnotCardinality, syntax_bitmapAndnotCardinality, arguments_bitmapAndnotCardinality, {}, returned_value_bitmapAndnotCardinality, examples_bitmapAndnotCardinality, introduced_in_bitmapAndnotCardinality, category_bitmapAndnotCardinality};

    factory.registerFunction<FunctionBitmapAndnotCardinality>(documentation_bitmapAndnotCardinality);

    /// Documentation for bitmapAnd
    FunctionDocumentation::Description description_bitmapAnd = "Computes the logical conjunction (AND) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAnd = "bitmapAnd(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAnd = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAnd = {"Returns a bitmap containing bits present in both input bitmaps", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapAnd = {{"Usage example", "SELECT bitmapToArray(bitmapAnd(bitmapBuild([1, 2, 3]), bitmapBuild([3, 4, 5]))) AS res;",
        R"(
┌─res─┐
│ [3] │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAnd = {20, 1};
    FunctionDocumentation::Category category_bitmapAnd = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAnd = {description_bitmapAnd, syntax_bitmapAnd, arguments_bitmapAnd, {}, returned_value_bitmapAnd, examples_bitmapAnd, introduced_in_bitmapAnd, category_bitmapAnd};

    factory.registerFunction<FunctionBitmapAnd>(documentation_bitmapAnd);

    /// Documentation for bitmapOr
    FunctionDocumentation::Description description_bitmapOr = "Computes the logical disjunction (OR) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapOr = "bitmapOr(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapOr = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapOr = {"Returns a bitmap containing set bits present in either input bitmap", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapOr = {{"Usage example", "SELECT bitmapToArray(bitmapOr(bitmapBuild([1, 2, 3]), bitmapBuild([3, 4, 5]))) AS res;",
        R"(
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapOr = {20, 1};
    FunctionDocumentation::Category category_bitmapOr = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapOr = {description_bitmapOr, syntax_bitmapOr, arguments_bitmapOr, {}, returned_value_bitmapOr, examples_bitmapOr, introduced_in_bitmapOr, category_bitmapOr};

    factory.registerFunction<FunctionBitmapOr>(documentation_bitmapOr);

    /// Documentation for bitmapXor
    FunctionDocumentation::Description description_bitmapXor = "Computes the symmetric difference (XOR) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapXor = "bitmapXor(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapXor = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapXor = {"Returns a bitmap containing set bits present in either input bitmap, but not in both", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapXor = {{"Usage example", "SELECT bitmapToArray(bitmapXor(bitmapBuild([1, 2, 3]), bitmapBuild([3, 4, 5]))) AS res;",
        R"(
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapXor = {20, 1};
    FunctionDocumentation::Category category_bitmapXor = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapXor = {description_bitmapXor, syntax_bitmapXor, arguments_bitmapXor, {}, returned_value_bitmapXor, examples_bitmapXor, introduced_in_bitmapXor, category_bitmapXor};

    factory.registerFunction<FunctionBitmapXor>(documentation_bitmapXor);

    /// Documentation for bitmapAndnot
    FunctionDocumentation::Description description_bitmapAndnot = "Computes the set difference A AND-NOT B of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAndnot = "bitmapAndnot(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAndnot = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAndnot = {"Returns a bitmap containing set bits present in the first bitmap but not in the second", {"AggregateFunction(groupBitmap, T)"}};
    FunctionDocumentation::Examples examples_bitmapAndnot = {{"Usage example", "SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1, 2, 3]), bitmapBuild([3, 4, 5]))) AS res;",
        R"(
┌─res───┐
│ [1,2] │
└───────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAndnot = {20, 1};
    FunctionDocumentation::Category category_bitmapAndnot = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAndnot = {description_bitmapAndnot, syntax_bitmapAndnot, arguments_bitmapAndnot, {}, returned_value_bitmapAndnot, examples_bitmapAndnot, introduced_in_bitmapAndnot, category_bitmapAndnot};

    factory.registerFunction<FunctionBitmapAndnot>(documentation_bitmapAndnot);

    /// Documentation for bitmapHasAll
    FunctionDocumentation::Description description_bitmapHasAll = "Checks if the first bitmap contains all set bits of the second bitmap.";
    FunctionDocumentation::Syntax syntax_bitmapHasAll = "bitmapHasAll(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapHasAll = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapHasAll = {"Returns `1` if all set bits of the second bitmap are present in the first bitmap, otherwise `0`", {"UInt8"}};
    FunctionDocumentation::Examples examples_bitmapHasAll = {{"Usage example", "SELECT bitmapHasAll(bitmapBuild([1, 2, 3]), bitmapBuild([2, 3])) AS res;",
        R"(
┌─res─┐
│   1 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapHasAll = {20, 1};
    FunctionDocumentation::Category category_bitmapHasAll = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapHasAll = {description_bitmapHasAll, syntax_bitmapHasAll, arguments_bitmapHasAll, {}, returned_value_bitmapHasAll, examples_bitmapHasAll, introduced_in_bitmapHasAll, category_bitmapHasAll};

    factory.registerFunction<FunctionBitmapHasAll>(documentation_bitmapHasAll);

    /// Documentation for bitmapHasAny
    FunctionDocumentation::Description description_bitmapHasAny = "Checks if the first bitmap contains any set bits of the second bitmap.";
    FunctionDocumentation::Syntax syntax_bitmapHasAny = "bitmapHasAny(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapHasAny = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapHasAny = {"Returns `1` if any bits of the second bitmap are present in the first bitmap, otherwise `0`", {"UInt8"}};
    FunctionDocumentation::Examples examples_bitmapHasAny = {{"Usage example", "SELECT bitmapHasAny(bitmapBuild([1, 2, 3]), bitmapBuild([3, 4, 5])) AS res;",
        R"(
┌─res─┐
│   1 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapHasAny = {20, 1};
    FunctionDocumentation::Category category_bitmapHasAny = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapHasAny = {description_bitmapHasAny, syntax_bitmapHasAny, arguments_bitmapHasAny, {}, returned_value_bitmapHasAny, examples_bitmapHasAny, introduced_in_bitmapHasAny, category_bitmapHasAny};

    factory.registerFunction<FunctionBitmapHasAny>(documentation_bitmapHasAny);

    /// Documentation for bitmapContains
    FunctionDocumentation::Description description_bitmapContains = "Checks if the bitmap contains a specific element. The value is compared as an unsigned integer of the bitmap element type. For signed bitmaps, a negative element matches its unsigned counterpart (for example, `Int8` value `-1` matches `255`).";
    FunctionDocumentation::Syntax syntax_bitmapContains = "bitmapContains(bitmap, value)";
    FunctionDocumentation::Arguments arguments_bitmapContains = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/reference/data-types/aggregatefunction)."},
        {"value", "Element to check for. [(U)Int8/16/32/64](/reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapContains = {"Returns `1` if the bitmap contains the specified value, otherwise `0`", {"UInt8"}};
    FunctionDocumentation::Examples examples_bitmapContains = {
        {"Usage example", "SELECT bitmapContains(bitmapBuild([1, 2, 3]), 2) AS res;",
        R"(
┌─res─┐
│   1 │
└─────┘
    )"},
        {"Signed bitmap", "SELECT bitmapContains(bitmapBuild([-1]::Array(Int8)), 255) AS res;",
        R"(
┌─res─┐
│   1 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapContains = {20, 1};
    FunctionDocumentation::Category category_bitmapContains = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapContains = {description_bitmapContains, syntax_bitmapContains, arguments_bitmapContains, {}, returned_value_bitmapContains, examples_bitmapContains, introduced_in_bitmapContains, category_bitmapContains};

    factory.registerFunction<FunctionBitmapContains>(documentation_bitmapContains);
}
}
