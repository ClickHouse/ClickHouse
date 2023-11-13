#include <Functions/FunctionFactory.h>

#include "FunctionsUniqTheta.h"

#if USE_DATASKETCHES

namespace DB
{

REGISTER_FUNCTION(UniqTheta)
{
    factory.registerFunction<FunctionUniqThetaIntersect>(
            {
              R"(
Two uniqThetaSketch objects to do intersect calculation(set operation ∩), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                    Documentation::Examples{
                            {"typical", "select finalizeAggregation(uniqThetaIntersect(arrayReduce('uniqThetaState',[1,2]), arrayReduce('uniqThetaState',[2,3,4])));"}},
                    Documentation::Categories{"uniqTheta"}
            });

    factory.registerFunction<FunctionUniqThetaUnion>(
            {
              R"(
Two uniqThetaSketch objects to do union calculation(set operation ∪), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                    Documentation::Examples{
                            {"typical", "select finalizeAggregation(uniqThetaUnion(arrayReduce('uniqThetaState',[1,2]), arrayReduce('uniqThetaState',[2,3,4])));"}},
                    Documentation::Categories{"uniqTheta"}
            });
    factory.registerFunction<FunctionUniqThetaNot>(
            {
              R"(
Two uniqThetaSketch objects to do a_not_b calculation(set operation ×), the result is a new uniqThetaSketch.

A uniqThetaSketch object is to be constructed by aggregation function uniqTheta with -State.

UniqThetaSketch is a data structure storage of approximate values set.
For more information on RoaringBitmap, see: [Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html).

Typical usage:
[example:typical]
)",
                    Documentation::Examples{
                            {"typical", "select finalizeAggregation(uniqThetaNot(arrayReduce('uniqThetaState',[1,2]), arrayReduce('uniqThetaState',[2,3,4])));"}},
                    Documentation::Categories{"uniqTheta"}
            });
}

}

#endif
