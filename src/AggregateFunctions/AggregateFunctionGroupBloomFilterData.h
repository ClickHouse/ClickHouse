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
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

static constexpr size_t BLOOM_FILTER_DEFAULT_SEED = 0;
static constexpr double BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE = 0.025;
static constexpr size_t BLOOM_FILTER_DEFAULT_EXPECTED_ELEMENTS = 10000;

/// Maximum allowed Bloom filter size in bytes (256 MB)
static constexpr size_t BLOOM_FILTER_MAX_SIZE_BYTES = 256 * 1024 * 1024;

static constexpr size_t BLOOM_FILTER_MAX_HASHES = 20;

/// Returns {filter_size_bytes, num_hashes} for the given expected element count and false-positive rate.
/// Standard path (k_opt = -ln(p)/ln2 <= BLOOM_FILTER_MAX_HASHES): m = -n*ln(p)/(ln2)^2, k = round(k_opt).
/// When k_opt > BLOOM_FILTER_MAX_HASHES, the standard size is too small to honour the requested FPR
/// with only BLOOM_FILTER_MAX_HASHES hash functions. In that case k is fixed at the cap and size is
/// derived from the inverse of p = (1-exp(-k*n/m))^k:
///   m = ceil( -k * n / ln(1 - p^(1/k)) )
inline std::pair<size_t, size_t> bloomFilterOptimalParams(size_t expected_elements, double false_positive_rate)
{
    if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "False positive rate for Bloom filter must be in (0, 1), got {}", false_positive_rate);
    if (expected_elements == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected number of elements for Bloom filter must be positive");

    const double n = static_cast<double>(expected_elements);
    const double ln2 = std::numbers::ln2;
    const double k_opt = -std::log(false_positive_rate) / ln2;

    size_t num_hashes = 0;
    double bytes_double = 0.0;
    if (k_opt <= static_cast<double>(BLOOM_FILTER_MAX_HASHES))
    {
        bytes_double = std::ceil(-n * std::log(false_positive_rate) / (ln2 * ln2) / 8.0);
        num_hashes = std::max(size_t{1}, static_cast<size_t>(std::round(k_opt)));
    }
    else
    {
        num_hashes = BLOOM_FILTER_MAX_HASHES;
        const double k = static_cast<double>(num_hashes);
        bytes_double = std::ceil(-k * n / std::log(1.0 - std::pow(false_positive_rate, 1.0 / k)) / 8.0);
    }

    if (!std::isfinite(bytes_double) || bytes_double > static_cast<double>(BLOOM_FILTER_MAX_SIZE_BYTES))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bloom filter parameters expected_elements={} and false_positive_rate={} require a filter "
            "larger than the maximum allowed size {} bytes, even with the maximum number of hash functions ({})",
            expected_elements, false_positive_rate, BLOOM_FILTER_MAX_SIZE_BYTES, BLOOM_FILTER_MAX_HASHES);

    size_t bytes = static_cast<size_t>(bytes_double);
    bytes = ((bytes + 7) / 8) * 8;  /// align to 64 bits
    return {bytes, num_hashes};
}


template <typename T>
T canonicalizeGroupBloomFilterValue(T value)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        /// IEEE 754 has multiple representations for zero and NaN; canonicalize so the filter
        /// never produces false negatives for equal values with different bit patterns.
        if (value == static_cast<T>(0))
            return static_cast<T>(0);
        if (std::isnan(value))
            return std::numeric_limits<T>::quiet_NaN();
    }

    return value;
}

struct AggregateFunctionGroupBloomFilterData
{
    static constexpr auto name = "groupBloomFilter";

    size_t filter_size_bytes = 0;
    size_t num_hashes = 0;
    size_t seed = BLOOM_FILTER_DEFAULT_SEED;
    std::unique_ptr<BloomFilter> bloom_filter;

    AggregateFunctionGroupBloomFilterData() = default;

    /// Store parameters without allocating the bitset.
    /// The BloomFilter is created lazily on the first call to add().
    void setParameters(size_t filter_size_bytes_, size_t num_hashes_, size_t seed_ = BLOOM_FILTER_DEFAULT_SEED)
    {
        filter_size_bytes = filter_size_bytes_;
        num_hashes = num_hashes_;
        seed = seed_;
    }

    void init(size_t filter_size_bytes_, size_t num_hashes_, size_t seed_ = BLOOM_FILTER_DEFAULT_SEED)
    {
        setParameters(filter_size_bytes_, num_hashes_, seed_);
        bloom_filter = std::make_unique<BloomFilter>(filter_size_bytes, num_hashes, seed);
    }

    bool isInitialized() const { return bloom_filter != nullptr; }

    void add(const char * data, size_t len) // NOLINT(readability-make-member-function-const)
    {
        /// Lazily allocate the bitset on the first inserted value so that
        /// empty/skipped groups (e.g. groupBloomFilterIfState(…)(x, 0)) never
        /// pay the allocation cost and serialize compactly with has_data = 0.
        if (!bloom_filter)
            bloom_filter = std::make_unique<BloomFilter>(filter_size_bytes, num_hashes, seed);
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

    /// Deserialize the state, validating the header against the declared aggregate function
    /// parameters (expected_filter_size_bytes, expected_num_hashes, expected_seed)
    void read(ReadBuffer & buf, size_t expected_filter_size_bytes, size_t expected_num_hashes, size_t expected_seed)
    {
        readVarUInt(filter_size_bytes, buf);
        readVarUInt(num_hashes, buf);
        readVarUInt(seed, buf);

        /// Absolute-bounds sanity checks (also protect the standalone / test path).
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

        /// Validate against the declared AggregateFunction parameters
        if (filter_size_bytes != expected_filter_size_bytes
            || num_hashes != expected_num_hashes
            || seed != expected_seed)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Bloom filter state parameters do not match declared aggregate function type: "
                "serialized size {}, hashes {}, seed {}; declared size {}, hashes {}, seed {}",
                filter_size_bytes, num_hashes, seed,
                expected_filter_size_bytes, expected_num_hashes, expected_seed);
        }

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
