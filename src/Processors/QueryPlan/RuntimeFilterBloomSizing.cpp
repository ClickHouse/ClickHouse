#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>

#include <algorithm>
#include <cmath>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
}

namespace
{

constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;
constexpr UInt64 MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 10;
constexpr UInt64 DEFAULT_RUNTIME_BLOOM_FILTER_BYTES = 512 * 1024;
constexpr UInt64 DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS = 3;
/// Max size up to which the Bloom filter grows before the false positive rate starts degrading.
constexpr UInt64 MAX_STATS_SIZED_BLOOM_FILTER_BYTES = 4 * 1024 * 1024;
/// At three hash functions this achieves a 12.5% false positive rate.
constexpr Float64 RUNTIME_BLOOM_FILTER_TARGET_FILL_RATE = 0.5;

}

RuntimeBloomFilterParameters resolveRuntimeBloomFilterDefaults(RuntimeBloomFilterParameters parameters) noexcept
{
    if (!parameters.bytes)
        parameters.bytes = DEFAULT_RUNTIME_BLOOM_FILTER_BYTES;
    if (!parameters.hash_functions)
        parameters.hash_functions = DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS;
    return parameters;
}

RuntimeBloomFilterParameters
sizeRuntimeBloomFilter(RuntimeBloomFilterParameters parameters, std::optional<UInt64> distinct_keys_hint, Float64 max_ratio_of_set_bits)
{
    parameters = resolveRuntimeBloomFilterDefaults(parameters);
    if (distinct_keys_hint)
        parameters.bytes
            = growRuntimeBloomFilterBytesFromStats(*distinct_keys_hint, parameters.hash_functions, parameters.bytes, max_ratio_of_set_bits);
    return parameters;
}

void validateRuntimeBloomFilterParameters(RuntimeBloomFilterParameters parameters)
{
    if (parameters.bytes > MAX_RUNTIME_BLOOM_FILTER_BYTES)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter size {} is too big, maximum: {}",
            parameters.bytes,
            MAX_RUNTIME_BLOOM_FILTER_BYTES);

    if (parameters.hash_functions > MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter hash function count {} is too big, maximum: {}",
            parameters.hash_functions,
            MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS);
}

Float64 estimateRuntimeBloomFilterSetBitsRatio(Float64 distinct_keys, RuntimeBloomFilterParameters parameters)
{
    parameters = resolveRuntimeBloomFilterDefaults(parameters);
    return -std::expm1(-static_cast<double>(parameters.hash_functions) * distinct_keys / (static_cast<double>(parameters.bytes) * 8.0));
}

UInt64 growRuntimeBloomFilterBytesFromStats(
    UInt64 distinct_keys, UInt64 hash_functions, UInt64 default_bloom_filter_bytes, Float64 max_ratio_of_set_bits)
{
    const Float64 target_fill_rate = std::min(RUNTIME_BLOOM_FILTER_TARGET_FILL_RATE, max_ratio_of_set_bits);
    const double ideal_bloom_filter_bytes
        = std::ceil(-static_cast<double>(hash_functions) * static_cast<double>(distinct_keys) / std::log1p(-target_fill_rate) / 8.0);
    const double clamped_bloom_filter_bytes
        = std::clamp(ideal_bloom_filter_bytes, 0.0, static_cast<double>(MAX_STATS_SIZED_BLOOM_FILTER_BYTES));
    return std::max(static_cast<UInt64>(clamped_bloom_filter_bytes), default_bloom_filter_bytes);
}

}
