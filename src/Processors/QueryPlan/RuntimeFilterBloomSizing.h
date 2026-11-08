#pragma once

#include <base/types.h>

namespace DB
{

/// Runtime bloom filter should be small and fast otherwise it is pointless
constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;
constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 10;

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
