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

/// The size `RuntimeBloomFilter` actually allocates for a byte budget: a power of two number of
/// 64-byte blocks, rounded down, but at least one block. Everything that predicts the fill rate of
/// the filter or compares its size must use this, not the raw budget.
UInt64 normalizeRuntimeBloomFilterBytes(UInt64 bytes) noexcept;

RuntimeBloomFilterParameters resolveRuntimeBloomFilterDefaults(RuntimeBloomFilterParameters parameters) noexcept;
void validateRuntimeBloomFilterParameters(RuntimeBloomFilterParameters parameters);

Float64 estimateRuntimeBloomFilterSetBitsRatio(Float64 distinct_keys, RuntimeBloomFilterParameters parameters);

UInt64 growRuntimeBloomFilterBytesFromStats(
    UInt64 distinct_keys, UInt64 hash_functions, UInt64 default_bloom_filter_bytes, Float64 max_ratio_of_set_bits);

}
