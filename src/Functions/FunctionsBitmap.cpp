#include <Functions/FunctionFactory.h>

/// TODO include this last because of a broken roaring header. See the comment inside.
#include <Functions/FunctionsBitmap.h>


namespace DB
{

REGISTER_FUNCTION(Bitmap)
{
    /// Documentation for bitmapBuild
    FunctionDocumentation::Description description_bitmapBuild = "Builds a bitmap from an unsigned integer array. It is the opposite of function [`bitmapToArray`](/sql-reference/functions/bitmap-functions#bitmapToArray).";
    FunctionDocumentation::Syntax syntax_bitmapBuild = "bitmapBuild(array)";
    FunctionDocumentation::Arguments arguments_bitmapBuild = {
        {"array", "Unsigned integer array. [`Array(UInt*)`](/sql-reference/data-types/array)."},
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapBuild = "Returns a bitmap from the provided array. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapBuild = {{"Usage example", "SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res, toTypeName(res);",
        R"(
┌─res─┬─toTypeName(bitmapBuild([1, 2, 3, 4, 5]))─────┐
│     │ AggregateFunction(groupBitmap, UInt8)        │
└─────┴──────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapBuild = {20, 1};
    FunctionDocumentation::Category category_bitmapBuild = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapBuild = {
        description_bitmapBuild,
        syntax_bitmapBuild,
        arguments_bitmapBuild,
        returned_value_bitmapBuild,
        examples_bitmapBuild,
        introduced_in_bitmapBuild,
        category_bitmapBuild
    };

    factory.registerFunction<FunctionBitmapBuild>(documentation_bitmapBuild);

    /// Documentation for bitmapToArray
    FunctionDocumentation::Description description_bitmapToArray = "Converts a bitmap to an array of unsigned integers. It is the opposite of function [`bitmapBuild`](/sql-reference/functions/bitmap-functions#bitmapbuild).";
    FunctionDocumentation::Syntax syntax_bitmapToArray = "bitmapToArray(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapToArray = {
        {"bitmap", "Bitmap to convert. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapToArray = "Returns an array of unsigned integers contained in the bitmap. [`Array(UInt*)`](/sql-reference/data-types/array)";
    FunctionDocumentation::Examples examples_bitmapToArray = {{"Usage example", "SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;",
        R"(
┌─res─────────────┐
│ [1, 2, 3, 4, 5] │
└─────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapToArray = {20, 1};
    FunctionDocumentation::Category category_bitmapToArray = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapToArray = {
        description_bitmapToArray,
        syntax_bitmapToArray,
        arguments_bitmapToArray,
        returned_value_bitmapToArray,
        examples_bitmapToArray,
        introduced_in_bitmapToArray,
        category_bitmapToArray
    };

    factory.registerFunction<FunctionBitmapToArray>(documentation_bitmapToArray);

    /// Documentation for bitmapSubsetInRange
    FunctionDocumentation::Description description_bitmapSubsetInRange = "Returns a subset of the bitmap, containing only the set bits in the specified range [start, end). Uses 1-based indexing.";
    FunctionDocumentation::Syntax syntax_bitmapSubsetInRange = "bitmapSubsetInRange(bitmap, start, end)";
    FunctionDocumentation::Arguments arguments_bitmapSubsetInRange = {
        {"bitmap", "Bitmap to extract the subset from. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"start", "Start of the range (inclusive). [`UInt*`](/sql-reference/data-types/int-uint)"},
        {"end", "End of the range (exclusive). [`UInt*`](/sql-reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapSubsetInRange = "Returns a bitmap containing only the set bits in the specified range. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapSubsetInRange = {{"Usage example", "SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([1, 2, 3, 4, 5]), 2, 5)) AS res;",
        R"(
┌─res───────┐
│ [2, 3, 4] │
└───────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapSubsetInRange = {20, 1};
    FunctionDocumentation::Category category_bitmapSubsetInRange = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapSubsetInRange = {
        description_bitmapSubsetInRange,
        syntax_bitmapSubsetInRange,
        arguments_bitmapSubsetInRange,
        returned_value_bitmapSubsetInRange,
        examples_bitmapSubsetInRange,
        introduced_in_bitmapSubsetInRange,
        category_bitmapSubsetInRange
    };

    factory.registerFunction<FunctionBitmapSubsetInRange>(documentation_bitmapSubsetInRange);

    /// Documentation for bitmapSubsetLimit
    FunctionDocumentation::Description description_bitmapSubsetLimit = "Returns a subset of a bitmap from position `range_start` with at most `cardinality_limit` set bits. Uses 1-based indexing.";
    FunctionDocumentation::Syntax syntax_bitmapSubsetLimit = "bitmapSubsetLimit(bitmap, range_start, cardinality_limit)";
    FunctionDocumentation::Arguments arguments_bitmapSubsetLimit = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"range_start", "Start of the range (inclusive). [`UInt32`](/sql-reference/data-types/int-uint)"},
        {"cardinality_limit", "Maximum cardinality of the subset. [`UInt32`](/sql-reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapSubsetLimit = "Returns a bitmap containing at most `cardinality_limit` set bits, starting from `range_start`. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapSubsetLimit = {{"Usage example", "SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([1, 5, 3, 2, 8]), 3, 2)) AS res;",
        R"(
┌─res────┐
│ [5, 3] │
└────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapSubsetLimit = {20, 1};
    FunctionDocumentation::Category category_bitmapSubsetLimit = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapSubsetLimit = {
        description_bitmapSubsetLimit,
        syntax_bitmapSubsetLimit,
        arguments_bitmapSubsetLimit,
        returned_value_bitmapSubsetLimit,
        examples_bitmapSubsetLimit,
        introduced_in_bitmapSubsetLimit,
        category_bitmapSubsetLimit
    };

    factory.registerFunction<FunctionBitmapSubsetLimit>(documentation_bitmapSubsetLimit);

    /// Documentation for subBitmap (bitmapSubsetOffsetLimit is the unregistered alias see: https://github.com/ClickHouse/ClickHouse/pull/27234)
    FunctionDocumentation::Description description_subBitmap = "Returns a subset of the bitmap, starting from position `offset`. The maximum cardinality of the returned bitmap is `cardinality_limit`.";
    FunctionDocumentation::Syntax syntax_subBitmap = "subBitmap(bitmap, offset, cardinality_limit)";
    FunctionDocumentation::Arguments arguments_subBitmap = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"offset", "Number of set bits to skip from the beginning (zero-based). [`UInt32`](/sql-reference/data-types/int-uint)"},
        {"cardinality_limit", "Maximum number of set bits to include in the subset. [`UInt32`](/sql-reference/data-types/int-uint)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_subBitmap = "Returns a bitmap containing at most `limit` set bits, starting after skipping `offset` set bits in ascending order. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_subBitmap = {{"Usage example", "SELECT bitmapToArray(subBitmap(bitmapBuild([1, 2, 3, 4, 5]), 2, 2)) AS res;",
        R"(
┌─res────┐
│ [3, 4] │
└────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subBitmap = {21, 9};
    FunctionDocumentation::Category category_subBitmap = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_subBitmap = {
        description_subBitmap,
        syntax_subBitmap,
        arguments_subBitmap,
        returned_value_subBitmap,
        examples_subBitmap,
        introduced_in_subBitmap,
        category_subBitmap
    };

    factory.registerFunction<FunctionBitmapSubsetOffsetLimit>(documentation_subBitmap);

    /// Documentation for bitmapTransform
    FunctionDocumentation::Description description_bitmapTransform = R"(
Changes up to N bits in a bitmap by swapping specific bit values in `from_array` with corresponding ones in `to_array`.
    )";
    FunctionDocumentation::Syntax syntax_bitmapTransform = "bitmapTransform(bitmap, from_array, to_array)";
    FunctionDocumentation::Arguments arguments_bitmapTransform =
    {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"from_array", "Array of original set bits to be replaced. [`Array(T)`](/sql-reference/data-types/array)."},
        {"to_array", "Array of new set bits to replace with. [`Array(T)`](/sql-reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapTransform = "Returns a bitmap with elements transformed according to the given mapping. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction).";
    FunctionDocumentation::Examples examples_bitmapTransform =
        {{"Usage example", "SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3, 4, 5]), [2, 4], [20, 40])) AS res;",
        R"(
┌─res───────────────┐
│ [1, 3, 5, 20, 40] │
└───────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapTransform = {20, 1};
    FunctionDocumentation::Category category_bitmapTransform = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapTransform =
    {
        description_bitmapTransform,
        syntax_bitmapTransform,
        arguments_bitmapTransform,
        returned_value_bitmapTransform,
        examples_bitmapTransform,
        introduced_in_bitmapTransform,
        category_bitmapTransform
    };

    factory.registerFunction<FunctionBitmapTransform>(documentation_bitmapTransform);

    factory.registerFunction<FunctionBitmapSelfCardinality>();
    factory.registerFunction<FunctionBitmapMin>();
    factory.registerFunction<FunctionBitmapMax>();
    factory.registerFunction<FunctionBitmapAndCardinality>();
    factory.registerFunction<FunctionBitmapOrCardinality>();
    factory.registerFunction<FunctionBitmapXorCardinality>();
    factory.registerFunction<FunctionBitmapAndnotCardinality>();

    factory.registerFunction<FunctionBitmapAnd>();
    factory.registerFunction<FunctionBitmapOr>();
    factory.registerFunction<FunctionBitmapXor>();
    factory.registerFunction<FunctionBitmapAndnot>();

    factory.registerFunction<FunctionBitmapHasAll>();
    factory.registerFunction<FunctionBitmapHasAny>();
    factory.registerFunction<FunctionBitmapContains>();
}
}
