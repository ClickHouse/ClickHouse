#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilter.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <numbers>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

/// Default parameters for Bloom filter
static constexpr size_t BLOOM_FILTER_DEFAULT_SEED = 0;
static constexpr double BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE = 0.025;
static constexpr size_t BLOOM_FILTER_DEFAULT_EXPECTED_ELEMENTS = 10000;

/// Maximum allowed Bloom filter size in bytes (256 MB)
static constexpr size_t BLOOM_FILTER_MAX_SIZE_BYTES = 256 * 1024 * 1024;

/// Maximum allowed number of hash functions for Bloom filter.
static constexpr size_t BLOOM_FILTER_MAX_HASHES = 20;

/// Compute optimal Bloom filter size in bytes given expected number of elements and false positive rate.
/// Formula: m = -n * ln(p) / (ln(2))^2
inline size_t bloomFilterOptimalSizeBytes(size_t expected_elements, double false_positive_rate)
{
    if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "False positive rate for Bloom filter must be in (0, 1), got {}", false_positive_rate);
    if (expected_elements == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected number of elements for Bloom filter must be positive");

    double ln2 = std::numbers::ln2;
    double bits = -static_cast<double>(expected_elements) * std::log(false_positive_rate) / (ln2 * ln2);
    double bytes_double = std::ceil(bits / 8.0);
    if (!std::isfinite(bytes_double) || bytes_double > static_cast<double>(BLOOM_FILTER_MAX_SIZE_BYTES))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bloom filter parameters expected_elements={} and false_positive_rate={} require a filter larger than the maximum allowed size {} bytes",
            expected_elements, false_positive_rate, BLOOM_FILTER_MAX_SIZE_BYTES);

    size_t bytes = static_cast<size_t>(bytes_double);
    /// Round up to multiple of 8 bytes (64 bits) for alignment
    bytes = ((bytes + 7) / 8) * 8;
    return bytes;
}

/// Compute optimal number of hash functions given filter size and expected elements.
/// Formula: k = (m / n) * ln(2)
inline size_t bloomFilterOptimalHashes(size_t filter_size_bytes, size_t expected_elements)
{
    if (expected_elements == 0)
        return 1;
    double bits_per_element = static_cast<double>(filter_size_bytes * 8) / static_cast<double>(expected_elements);
    size_t hashes = static_cast<size_t>(std::round(bits_per_element * std::numbers::ln2));
    if (hashes == 0)
        hashes = 1;
    hashes = std::min(hashes, BLOOM_FILTER_MAX_HASHES);
    return hashes;
}


template <typename T>
T canonicalizeGroupBloomFilterValue(T value)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        /// IEEE 754 has multiple object representations for values that ClickHouse treats as equal
        /// or equivalent for set-membership purposes. Hash a canonical representation so the Bloom
        /// filter keeps the advertised no-false-negatives property for `Float32` and `Float64`.
        if (value == static_cast<T>(0))
            return static_cast<T>(0);
        if (std::isnan(value))
            return std::numeric_limits<T>::quiet_NaN();
    }

    return value;
}

/// Data structure for groupBloomFilter aggregate function.
/// Wraps BloomFilter from src/Interpreters/BloomFilter.h for use in aggregate functions.
struct AggregateFunctionGroupBloomFilterData
{
    static constexpr auto name = "groupBloomFilter";

    size_t filter_size_bytes = 0;
    size_t num_hashes = 0;
    size_t seed = BLOOM_FILTER_DEFAULT_SEED;
    std::unique_ptr<BloomFilter> bloom_filter;

    AggregateFunctionGroupBloomFilterData() = default;

    void init(size_t filter_size_bytes_, size_t num_hashes_, size_t seed_ = BLOOM_FILTER_DEFAULT_SEED)
    {
        filter_size_bytes = filter_size_bytes_;
        num_hashes = num_hashes_;
        seed = seed_;
        bloom_filter = std::make_unique<BloomFilter>(filter_size_bytes, num_hashes, seed);
    }

    bool isInitialized() const { return bloom_filter != nullptr; }

    void add(const char * data, size_t len) // NOLINT(readability-make-member-function-const)
    {
        if (bloom_filter)
            bloom_filter->add(data, len);
    }

    bool contains(const char * data, size_t len) const
    {
        if (!bloom_filter)
            return false;
        return bloom_filter->find(data, len);
    }

    void merge(const AggregateFunctionGroupBloomFilterData & rhs)
    {
        if (!rhs.bloom_filter)
            return;

        if (!bloom_filter)
        {
            filter_size_bytes = rhs.filter_size_bytes;
            num_hashes = rhs.num_hashes;
            seed = rhs.seed;
            bloom_filter = std::make_unique<BloomFilter>(filter_size_bytes, num_hashes, seed);
        }
        else if (filter_size_bytes != rhs.filter_size_bytes || num_hashes != rhs.num_hashes || seed != rhs.seed)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot merge Bloom filters with different parameters: "
                "size ({} vs {}), hashes ({} vs {}), seed ({} vs {})",
                filter_size_bytes, rhs.filter_size_bytes,
                num_hashes, rhs.num_hashes,
                seed, rhs.seed);
        }

        /// Merge by OR-ing the filter arrays
        auto & lhs_filter = bloom_filter->getFilter();
        const auto & rhs_filter = rhs.bloom_filter->getFilter();
        for (size_t i = 0; i < lhs_filter.size(); ++i)
            lhs_filter[i] |= rhs_filter[i];
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(filter_size_bytes, buf);
        writeVarUInt(num_hashes, buf);
        writeVarUInt(seed, buf);
        if (bloom_filter)
        {
            writeBinary(UInt8(1), buf);
            const auto & filter = bloom_filter->getFilter();
            buf.write(reinterpret_cast<const char *>(filter.data()), filter.size() * sizeof(BloomFilter::UnderType));
        }
        else
        {
            writeBinary(UInt8(0), buf);
        }
    }

    void read(ReadBuffer & buf)
    {
        readVarUInt(filter_size_bytes, buf);
        readVarUInt(num_hashes, buf);
        readVarUInt(seed, buf);

        if (filter_size_bytes == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Bloom filter size cannot be zero");
        if (filter_size_bytes > BLOOM_FILTER_MAX_SIZE_BYTES)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Bloom filter size {} exceeds maximum allowed size {}",
                filter_size_bytes, BLOOM_FILTER_MAX_SIZE_BYTES);
        if (num_hashes == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Number of hash functions cannot be zero");
        if (num_hashes > BLOOM_FILTER_MAX_HASHES)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Number of hash functions {} exceeds maximum allowed {}",
                num_hashes, BLOOM_FILTER_MAX_HASHES);

        UInt8 has_data = 0;
        readBinary(has_data, buf);
        if (has_data > 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Bloom filter state data flag: {}", static_cast<UInt16>(has_data));

        if (has_data)
        {
            bloom_filter = std::make_unique<BloomFilter>(filter_size_bytes, num_hashes, seed);
            auto & filter = bloom_filter->getFilter();
            buf.readStrict(reinterpret_cast<char *>(filter.data()), filter.size() * sizeof(BloomFilter::UnderType));
        }
        else
        {
            bloom_filter.reset();
        }
    }
};

}
