#include <Functions/FunctionFactory.h>

#include "FunctionsUniqTheta.h"

#if USE_DATASKETCHES

namespace DB
{

REGISTER_FUNCTION(UniqTheta)
{
    factory.registerFunction<FunctionUniqThetaIntersect>(
            FunctionDocumentation{
                .description = R"(
Two uniqThetaSketch objects to do intersect calculation(set operation ∩), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
                )",
                .syntax="uniqThetaIntersect(x, y)",
                .arguments={
                    {"x", "First uniqThetaSketch object."},
                    {"y", "Second uniqThetaSketch object."}
                },
                .returned_value="Returns `x ∩ y`, a new uniqThetaSketch.",
                .examples{
                    {
                        "typical",
                        R"(
SELECT
    finalizeAggregation(uniqThetaIntersect(a, b)) AS a_intersect_b,
    finalizeAggregation(a) AS a_cardinality,
    finalizeAggregation(b) AS b_cardinality
FROM
(
    SELECT
        arrayReduce('uniqThetaState', [1, 2]) AS a,
        arrayReduce('uniqThetaState', [2, 3, 4]) AS b
)
                        )",
                        R"(
┌─a_intersect_b─┬─a_cardinality─┬─b_cardinality─┐
│             1 │             2 │             3 │
└───────────────┴───────────────┴───────────────┘
                        )"
                    }
                },
                .category=FunctionDocumentation::Category::Other
            });

    factory.registerFunction<FunctionUniqThetaUnion>(
            FunctionDocumentation{
                .description = R"(
Two uniqThetaSketch objects to do union calculation(set operation ∪), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
                )",
                .syntax="uniqThetaUnion(x, y)",
                .arguments={
                    {"x", "First uniqThetaSketch object."},
                    {"y", "Second uniqThetaSketch object."}
                },
                .returned_value="Returns `x ∪ y`, a new uniqThetaSketch.",
                .examples{
                    {
                        "typical",
                        R"(
SELECT
    finalizeAggregation(uniqThetaUnion(a, b)) AS a_union_b,
    finalizeAggregation(a) AS a_cardinality,
    finalizeAggregation(b) AS b_cardinality
FROM
(
    SELECT
        arrayReduce('uniqThetaState', [1, 2]) AS a,
        arrayReduce('uniqThetaState', [2, 3, 4]) AS b
)
                        )",
                        R"(
┌─a_union_b─┬─a_cardinality─┬─b_cardinality─┐
│         4 │             2 │             3 │
└───────────┴───────────────┴───────────────┘
                        )"
                    }
                },
                .category=FunctionDocumentation::Category::Other
            });
    factory.registerFunction<FunctionUniqThetaNot>(
            FunctionDocumentation{
                .description = R"(
Two uniqThetaSketch objects to do a_not_b calculation(set operation ×), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                .syntax="uniqThetaNot(x, y)",
                .arguments={
                    {"x", "First uniqThetaSketch object."},
                    {"y", "Second uniqThetaSketch object."}
                },
                .returned_value="Returns `x × y`, a new uniqThetaSketch.",
                .examples{
                    {
                        "typical",
                        R"(
SELECT
    finalizeAggregation(uniqThetaNot(a, b)) AS a_not_b,
    finalizeAggregation(a) AS a_cardinality,
    finalizeAggregation(b) AS b_cardinality
FROM
(
    SELECT
        arrayReduce('uniqThetaState', [2, 3, 4]) AS a,
        arrayReduce('uniqThetaState', [1, 2]) AS b
)
                        )",
                        R"(
┌─a_not_b─┬─a_cardinality─┬─b_cardinality─┐
│       2 │             3 │             2 │
└─────────┴───────────────┴───────────────┘
                        )"
                    }
                },
                .category=FunctionDocumentation::Category::Other
            });
}

}

#endif
