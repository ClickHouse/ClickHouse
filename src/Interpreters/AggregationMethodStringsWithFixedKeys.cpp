#include <Interpreters/AggregationMethodStringsWithFixedKeys.h>
#include <Interpreters/AggregatedData.h>

namespace DB
{

template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKey<UInt8>, UInt8, true, true>;
template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKey<UInt16>, UInt16, true, true>;
template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKey<UInt32>, UInt32, true, true>;
template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKey<UInt64>, UInt64, true, true>;

template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKeyTwoLevel<UInt8>, UInt8, true, true>;
template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKeyTwoLevel<UInt16>, UInt16, true, true>;
template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKeyTwoLevel<UInt32>, UInt32, true, true>;
template struct AggregationMethodStringsWithFixedKeys<AggregatedDataWithStringAndFixedKeyTwoLevel<UInt64>, UInt64, true, true>;

}
