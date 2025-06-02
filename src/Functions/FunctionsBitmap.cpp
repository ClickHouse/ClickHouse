#include <Functions/FunctionFactory.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <Functions/FunctionsBitmap.h>


namespace DB
{

REGISTER_FUNCTION(Bitmap)
{
    factory.registerFunction<FunctionBitmapBuild>();
    factory.registerFunction<FunctionBitmapToArray>();
    factory.registerFunction<FunctionBitmapSubsetInRange>();
    factory.registerFunction<FunctionBitmapSubsetLimit>();
    factory.registerFunction<FunctionBitmapSubsetOffsetLimit>();
    factory.registerFunction<FunctionBitmapTransform>();

    /// Documentation for bitmapCardinality
    FunctionDocumentation::Description description_bitmapCardinality = "Returns the number of bits set (the cardinality) in the bitmap.";
    FunctionDocumentation::Syntax syntax_bitmapCardinality = "bitmapCardinality(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapCardinality = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapCardinality = "Returns the number of bits set in the bitmap. [`UInt64`](/sql-reference/data-types/int-uint)";
    FunctionDocumentation::Examples examples_bitmapCardinality = {{"Usage example", "SELECT bitmapCardinality(bitmapBuild([1, 3, 3, 5, 7, 7])) AS res",
        R"(
┌─res─┐
│   4 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapCardinality = {
        description_bitmapCardinality,
        syntax_bitmapCardinality,
        arguments_bitmapCardinality,
        returned_value_bitmapCardinality,
        examples_bitmapCardinality,
        introduced_in_bitmapCardinality,
        category_bitmapCardinality
    };

    factory.registerFunction<FunctionBitmapSelfCardinality>(documentation_bitmapCardinality);

    /// Documentation for bitmapMin
    FunctionDocumentation::Description description_bitmapMin = "Returns the position of the smallest bit set in a bitmap. If all bits are unset, or `UINT32_MAX` (`UINT64_MAX` if the bitmap contains more than `2^64` bits).";
    FunctionDocumentation::Syntax syntax_bitmapMin = "bitmapMin(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapMin = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapMin = "Returns the position of the smallest bit set in the bitmap, or `UINT32_MAX`/`UINT64_MAX`. [`UInt64`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_bitmapMin = {{"Usage example", "SELECT bitmapMin(bitmapBuild([3, 5, 2, 6])) AS res;",
        R"(
┌─res─┐
│   2 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapMin = {20, 1};
    FunctionDocumentation::Category category_bitmapMin = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapMin = {
        description_bitmapMin,
        syntax_bitmapMin,
        arguments_bitmapMin,
        returned_value_bitmapMin,
        examples_bitmapMin,
        introduced_in_bitmapMin,
        category_bitmapMin
    };

    factory.registerFunction<FunctionBitmapMin>(documentation_bitmapMin);

    /// Documentation for bitmapMax
    FunctionDocumentation::Description description_bitmapMax = "Returns the position of the greatest bit set in a bitmap, or `0` if the bitmap is empty.";
    FunctionDocumentation::Syntax syntax_bitmapMax = "bitmapMax(bitmap)";
    FunctionDocumentation::Arguments arguments_bitmapMax = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapMax = "Returns the position of the greatest bit set in the bitmap, otherwise `0`. [`UInt64`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_bitmapMax = {{"Usage example", "SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res;",
        R"(
┌─res─┐
│   5 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapMax = {20, 1};
    FunctionDocumentation::Category category_bitmapMax = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapMax = {
        description_bitmapMax,
        syntax_bitmapMax,
        arguments_bitmapMax,
        returned_value_bitmapMax,
        examples_bitmapMax,
        introduced_in_bitmapMax,
        category_bitmapMax
    };

    factory.registerFunction<FunctionBitmapMax>(documentation_bitmapMax);

    /// Documentation for bitmapAndCardinality
    FunctionDocumentation::Description description_bitmapAndCardinality = "Returns the cardinality of the logical conjunction (AND) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAndCardinality = "bitmapAndCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAndCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAndCardinality = "Returns the number of set bits in the intersection of the two bitmaps. [`UInt64`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_bitmapAndCardinality = {{"Usage example", "SELECT bitmapAndCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   1 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAndCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapAndCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAndCardinality = {
        description_bitmapAndCardinality,
        syntax_bitmapAndCardinality,
        arguments_bitmapAndCardinality,
        returned_value_bitmapAndCardinality,
        examples_bitmapAndCardinality,
        introduced_in_bitmapAndCardinality,
        category_bitmapAndCardinality
    };

    factory.registerFunction<FunctionBitmapAndCardinality>(documentation_bitmapAndCardinality);

    /// Documentation for bitmapOrCardinality
    FunctionDocumentation::Description description_bitmapOrCardinality = "Returns the cardinality of the logical disjunction (OR) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapOrCardinality = "bitmapOrCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapOrCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapOrCardinality = "Returns the number of set bits in the union of the two bitmaps. [`UInt64`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_bitmapOrCardinality = {{"Usage example", "SELECT bitmapOrCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   5 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapOrCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapOrCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapOrCardinality = {
        description_bitmapOrCardinality,
        syntax_bitmapOrCardinality,
        arguments_bitmapOrCardinality,
        returned_value_bitmapOrCardinality,
        examples_bitmapOrCardinality,
        introduced_in_bitmapOrCardinality,
        category_bitmapOrCardinality
    };

    factory.registerFunction<FunctionBitmapOrCardinality>(documentation_bitmapOrCardinality);

    /// Documentation for bitmapXorCardinality
    FunctionDocumentation::Description description_bitmapXorCardinality = "Returns the cardinality of the XOR (symmetric difference) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapXorCardinality = "bitmapXorCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapXorCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapXorCardinality = "Returns the number of set bits in the symmetric difference of the two bitmaps. [`UInt64`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_bitmapXorCardinality = {{"Usage example", "SELECT bitmapXorCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   4 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapXorCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapXorCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapXorCardinality = {
        description_bitmapXorCardinality,
        syntax_bitmapXorCardinality,
        arguments_bitmapXorCardinality,
        returned_value_bitmapXorCardinality,
        examples_bitmapXorCardinality,
        introduced_in_bitmapXorCardinality,
        category_bitmapXorCardinality
    };

    factory.registerFunction<FunctionBitmapXorCardinality>(documentation_bitmapXorCardinality);

    /// Documentation for bitmapAndnotCardinality
    FunctionDocumentation::Description description_bitmapAndnotCardinality = "Returns the cardinality of the AND-NOT operation of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAndnotCardinality = "bitmapAndnotCardinality(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAndnotCardinality = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAndnotCardinality = "Returns the number of set bits in the result of `bitmap1 AND-NOT bitmap2`. [`UInt64`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_bitmapAndnotCardinality = {{"Usage example", "SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│   2 │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAndnotCardinality = {20, 1};
    FunctionDocumentation::Category category_bitmapAndnotCardinality = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAndnotCardinality = {
        description_bitmapAndnotCardinality,
        syntax_bitmapAndnotCardinality,
        arguments_bitmapAndnotCardinality,
        returned_value_bitmapAndnotCardinality,
        examples_bitmapAndnotCardinality,
        introduced_in_bitmapAndnotCardinality,
        category_bitmapAndnotCardinality
    };

    factory.registerFunction<FunctionBitmapAndnotCardinality>(documentation_bitmapAndnotCardinality);

    factory.registerFunction<FunctionBitmapAnd>();
    factory.registerFunction<FunctionBitmapOr>();
    factory.registerFunction<FunctionBitmapXor>();
    factory.registerFunction<FunctionBitmapAndnot>();

    factory.registerFunction<FunctionBitmapHasAll>();
    factory.registerFunction<FunctionBitmapHasAny>();
    factory.registerFunction<FunctionBitmapContains>();
}
}
