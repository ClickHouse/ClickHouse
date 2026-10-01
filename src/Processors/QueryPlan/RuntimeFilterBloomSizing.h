#pragma once

#include <base/types.h>

namespace DB
{

struct RuntimeBloomFilterParameters
{
    UInt64 bytes;
    UInt64 hash_functions;
};

RuntimeBloomFilterParameters resolveRuntimeBloomFilterDefaults(RuntimeBloomFilterParameters parameters) noexcept;
void validateRuntimeBloomFilterParameters(RuntimeBloomFilterParameters parameters);

Float64 estimateRuntimeBloomFilterSetBitsRatio(Float64 distinct_keys, RuntimeBloomFilterParameters parameters);

UInt64 growRuntimeBloomFilterBytesFromStats(
    UInt64 distinct_keys, UInt64 hash_functions, UInt64 default_bloom_filter_bytes, Float64 max_ratio_of_set_bits);

}
