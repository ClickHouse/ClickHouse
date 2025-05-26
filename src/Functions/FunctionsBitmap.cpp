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

    factory.registerFunction<FunctionBitmapSelfCardinality>();
    factory.registerFunction<FunctionBitmapMin>();
    factory.registerFunction<FunctionBitmapMax>();
    factory.registerFunction<FunctionBitmapAndCardinality>();
    factory.registerFunction<FunctionBitmapOrCardinality>();
    factory.registerFunction<FunctionBitmapXorCardinality>();
    factory.registerFunction<FunctionBitmapAndnotCardinality>();

    // Documentation for bitmapAnd
    FunctionDocumentation::Description description_bitmapAnd = "Computes the logical conjunction (AND) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapAnd = "bitmapAnd(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAnd = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAnd = "Returns a bitmap containing elements present in both input bitmaps. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapAnd = {{"Usage example", "SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]), bitmapBuild([3,4,5]))) AS res;",
        R"(
┌─res─┐
│ [3] │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAnd = {20, 1};
    FunctionDocumentation::Category category_bitmapAnd = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAnd = {
        description_bitmapAnd,
        syntax_bitmapAnd,
        arguments_bitmapAnd,
        returned_value_bitmapAnd,
        examples_bitmapAnd,
        introduced_in_bitmapAnd,
        category_bitmapAnd
    };

    factory.registerFunction<FunctionBitmapAnd>(documentation_bitmapAnd);

    // Documentation for bitmapOr
    FunctionDocumentation::Description description_bitmapOr = "Computes the logical disjunction (OR) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapOr = "bitmapOr(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapOr = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapOr = "Returns a bitmap containing elements present in either input bitmap. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapOr = {{"Usage example", "SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]), bitmapBuild([3,4,5]))) AS res;",
        R"(
┌─res─────────┐
│ [1,2,3,4,5] │
└─────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapOr = {20, 1};
    FunctionDocumentation::Category category_bitmapOr = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapOr = {
        description_bitmapOr,
        syntax_bitmapOr,
        arguments_bitmapOr,
        returned_value_bitmapOr,
        examples_bitmapOr,
        introduced_in_bitmapOr,
        category_bitmapOr
    };

    factory.registerFunction<FunctionBitmapOr>(documentation_bitmapOr);

    // Documentation for bitmapXor
    FunctionDocumentation::Description description_bitmapXor = "Computes the symmetric difference (XOR) of two bitmaps.";
    FunctionDocumentation::Syntax syntax_bitmapXor = "bitmapXor(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapXor = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapXor = "Returns a bitmap containing elements present in either input bitmap, but not in both. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapXor = {{"Usage example", "SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]), bitmapBuild([3,4,5]))) AS res;",
        R"(
┌─res───────┐
│ [1,2,4,5] │
└───────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapXor = {20, 1};
    FunctionDocumentation::Category category_bitmapXor = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapXor = {
        description_bitmapXor,
        syntax_bitmapXor,
        arguments_bitmapXor,
        returned_value_bitmapXor,
        examples_bitmapXor,
        introduced_in_bitmapXor,
        category_bitmapXor
    };

    factory.registerFunction<FunctionBitmapXor>(documentation_bitmapXor);

    // Documentation for bitmapAndnot
    FunctionDocumentation::Description description_bitmapAndnot = "Computes the logical conjunction of two bitmaps and negates the result (AND-NOT).";
    FunctionDocumentation::Syntax syntax_bitmapAndnot = "bitmapAndnot(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapAndnot = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapAndnot = "Returns a bitmap containing elements present in the first bitmap but not in the second. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)";
    FunctionDocumentation::Examples examples_bitmapAndnot = {{"Usage example", "SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]), bitmapBuild([3,4,5]))) AS res;",
        R"(
┌─res───┐
│ [1,2] │
└───────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapAndnot = {20, 1};
    FunctionDocumentation::Category category_bitmapAndnot = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapAndnot = {
        description_bitmapAndnot,
        syntax_bitmapAndnot,
        arguments_bitmapAndnot,
        returned_value_bitmapAndnot,
        examples_bitmapAndnot,
        introduced_in_bitmapAndnot,
        category_bitmapAndnot
    };

    factory.registerFunction<FunctionBitmapAndnot>(documentation_bitmapAndnot);

    // Documentation for bitmapHasAll
    FunctionDocumentation::Description description_bitmapHasAll = "Checks if the first bitmap contains all elements of the second bitmap.";
    FunctionDocumentation::Syntax syntax_bitmapHasAll = "bitmapHasAll(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapHasAll = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapHasAll = "Returns `1` if all elements of the second bitmap are present in the first bitmap, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_bitmapHasAll = {{"Usage example", "SELECT bitmapHasAll(bitmapBuild([1,2,3]), bitmapBuild([2,3])) AS res;",
        R"(
┌─res─┐
│  1  │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapHasAll = {20, 1};
    FunctionDocumentation::Category category_bitmapHasAll = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapHasAll = {
        description_bitmapHasAll,
        syntax_bitmapHasAll,
        arguments_bitmapHasAll,
        returned_value_bitmapHasAll,
        examples_bitmapHasAll,
        introduced_in_bitmapHasAll,
        category_bitmapHasAll
    };

    factory.registerFunction<FunctionBitmapHasAll>(documentation_bitmapHasAll);

    // Documentation for bitmapHasAny
    FunctionDocumentation::Description description_bitmapHasAny = "Checks if the first bitmap contains any elements of the second bitmap.";
    FunctionDocumentation::Syntax syntax_bitmapHasAny = "bitmapHasAny(bitmap1, bitmap2)";
    FunctionDocumentation::Arguments arguments_bitmapHasAny = {
        {"bitmap1", "First bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"bitmap2", "Second bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapHasAny = "Returns `1` if any element of the second bitmap is present in the first bitmap, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_bitmapHasAny = {{"Usage example", "SELECT bitmapHasAny(bitmapBuild([1,2,3]), bitmapBuild([3,4,5])) AS res;",
        R"(
┌─res─┐
│  1  │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapHasAny = {20, 1};
    FunctionDocumentation::Category category_bitmapHasAny = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapHasAny = {
        description_bitmapHasAny,
        syntax_bitmapHasAny,
        arguments_bitmapHasAny,
        returned_value_bitmapHasAny,
        examples_bitmapHasAny,
        introduced_in_bitmapHasAny,
        category_bitmapHasAny
    };

    factory.registerFunction<FunctionBitmapHasAny>(documentation_bitmapHasAny);

    // Documentation for bitmapContains
    FunctionDocumentation::Description description_bitmapContains = "Checks if the bitmap contains a specific element.";
    FunctionDocumentation::Syntax syntax_bitmapContains = "bitmapContains(bitmap, value)";
    FunctionDocumentation::Arguments arguments_bitmapContains = {
        {"bitmap", "Bitmap object. [`AggregateFunction(groupBitmap, T)`](/sql-reference/data-types/aggregatefunction)."},
        {"value", "Element to check for. [(U)Int8/16/32/64](/sql-reference/data-types/int-uint/)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_bitmapContains = "Returns `1` if the bitmap contains the specified value, otherwise `0`. [`UInt8`](/sql-reference/data-types/int-uint/)";
    FunctionDocumentation::Examples examples_bitmapContains = {{"Usage example", "SELECT bitmapContains(bitmapBuild([1,2,3]), 2) AS res;",
        R"(
┌─res─┐
│  1  │
└─────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bitmapContains = {20, 1};
    FunctionDocumentation::Category category_bitmapContains = FunctionDocumentation::Category::Bitmap;
    FunctionDocumentation documentation_bitmapContains = {
        description_bitmapContains,
        syntax_bitmapContains,
        arguments_bitmapContains,
        returned_value_bitmapContains,
        examples_bitmapContains,
        introduced_in_bitmapContains,
        category_bitmapContains
    };

    factory.registerFunction<FunctionBitmapContains>(documentation_bitmapContains);
}
}
