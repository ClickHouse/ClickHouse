#pragma once

#include <limits>
#include <algorithm>
#include <climits>
#include <AggregateFunctions/ReservoirSampler.h>
#include <base/types.h>
#include <base/sort.h>
#include <Common/HashTable/Hash.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
#include <Common/NaNUtils.h>
#include <Poco/Exception.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}
}

/// Implementation of Reservoir Sampling algorithm. Incrementally selects from the added objects a random subset of the `sample_count` size.
/// Can approximately get quantiles.
/// The `quantile` call takes O(sample_count log sample_count), if after the previous call `quantile` there was at least one call to insert. Otherwise, O(1).
/// That is, it makes sense to first add, then get quantiles without adding.


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}
}


namespace detail
{
    const size_t DEFAULT_MAX_SAMPLE_SIZE = 8192;
    const auto MAX_SKIP_DEGREE = sizeof(UInt32) * 8;
}

/// What if there is not a single value - throw an exception, or return 0 or NaN in the case of double?
enum class ReservoirSamplerDeterministicOnEmpty : uint8_t
{
    THROW,
    RETURN_NAN_OR_ZERO,
};


template <typename T,
    ReservoirSamplerDeterministicOnEmpty OnEmpty = ReservoirSamplerDeterministicOnEmpty::THROW>
class ReservoirSamplerDeterministic
{
private:
    bool good(UInt32 hash) const
    {
        return (hash & skip_mask) == 0;
    }

public:
    explicit ReservoirSamplerDeterministic(const size_t max_sample_size_ = detail::DEFAULT_MAX_SAMPLE_SIZE)
        : max_sample_size{max_sample_size_}
    {
    }

    void clear()
    {
        samples.clear();
        sorted = false;
        total_values = 0;
    }

    void insert(const T & v, UInt64 determinator)
    {
        if (isNaN(v))
            return;

        UInt32 hash = static_cast<UInt32>(intHash64(determinator));
        insertImpl(v, hash);
        sorted = false;
        ++total_values;
    }

    size_t size() const
    {
        return total_values;
    }

    bool empty() const
    {
        return samples.empty();
    }

    T quantileNearest(double level)
    {
        if (samples.empty())
            return onEmpty<T>();

        sortIfNeeded();

        double index = level * (samples.size() - 1);
        size_t int_index = static_cast<size_t>(index + 0.5); /// NOLINT
        int_index = std::max(0LU, std::min(samples.size() - 1, int_index));
        return samples[int_index].first;
    }

    /** If T is not a numeric type, using this method causes a compilation error,
      *  but use of error class does not cause. SFINAE.
      *  Not SFINAE. Functions members of type templates are simply not checked until they are used.
      */
    double quantileInterpolated(double level)
    {
        if (samples.empty())
            return onEmpty<double>();

        sortIfNeeded();

        const double index = std::max(0., std::min(samples.size() - 1., level * (samples.size() - 1)));

        /// To get a value from a fractional index, we linearly interpolate between adjacent values.
        size_t left_index = static_cast<size_t>(index);
        size_t right_index = left_index + 1;
        if (right_index == samples.size())
            return static_cast<double>(samples[left_index].first);

        const double left_coef = right_index - index;
        const double right_coef = index - left_index;

        return static_cast<double>(samples[left_index].first) * left_coef + static_cast<double>(samples[right_index].first) * right_coef;
    }

    void merge(const ReservoirSamplerDeterministic & b)
    {
        if (max_sample_size != b.max_sample_size)
            throw Poco::Exception("Cannot merge ReservoirSamplerDeterministic's with different max sample size");
        sorted = false;

        if (skip_degree < b.skip_degree)
            setSkipDegree(b.skip_degree);

        for (const auto & sample : b.samples)
            insertImpl(sample.first, sample.second);

        total_values += b.total_values;
    }

    void read(DB::ReadBuffer & buf)
    {
        size_t size = 0;
        readBinaryLittleEndian(size, buf);
        readBinaryLittleEndian(total_values, buf);

        /// Compatibility with old versions.
        size = std::min(size, total_values);

        static constexpr size_t MAX_RESERVOIR_SIZE = 1_GiB;
        if (unlikely(size > MAX_RESERVOIR_SIZE))
            throw DB::Exception(DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                                "Too large array size (maximum: {})", MAX_RESERVOIR_SIZE);

        samples.resize(size);
        for (size_t i = 0; i < size; ++i)
            readBinaryLittleEndian(samples[i], buf);

        sorted = false;
    }

    void write(DB::WriteBuffer & buf) const
    {
        const size_t size = samples.size();
        writeBinaryLittleEndian(size, buf);
        writeBinaryLittleEndian(total_values, buf);

        for (size_t i = 0; i < size; ++i)
        {
            /// There was a mistake in this function.
            /// Instead of correctly serializing the elements,
            ///  it was writing them with uninitialized padding.
            /// Here we ensure that padding is zero without changing the protocol.
            /// TODO: After implementation of "versioning aggregate function state",
            /// change the serialization format.
            Element elem;
            memset(&elem, 0, sizeof(elem)); /// NOLINT(bugprone-undefined-memory-manipulation)
            elem = samples[i];

            DB::transformEndianness<std::endian::little>(elem);
            DB::writeString(reinterpret_cast<const char*>(&elem), sizeof(elem), buf);
        }
    }

private:
    /// We allocate some memory on the stack to avoid allocations when there are many objects with a small number of elements.
    using Element = std::pair<T, UInt32>;
    using Array = DB::PODArray<Element, 64>;

    const size_t max_sample_size; /// Maximum amount of stored values.
    size_t total_values = 0;   /// How many values were inserted (regardless if they remain in sample or not).
    bool sorted = false;
    Array samples;

    /// The number N determining that we store only one per 2^N elements in average.
    UInt8 skip_degree = 0;

    /// skip_mask is calculated as (2 ^ skip_degree - 1). We store an element only if (hash & skip_mask) == 0.
    /// For example, if skip_degree==0 then skip_mask==0 means we store each element;
    /// if skip_degree==1 then skip_mask==0b0001 means we store one per 2 elements in average;
    /// if skip_degree==4 then skip_mask==0b1111 means we store one per 16 elements in average.
    UInt32 skip_mask = 0;

    void insertImpl(const T & v, const UInt32 hash)
    {
        if (!good(hash))
            return;

        /// Make a room for plus one element.
        while (samples.size() >= max_sample_size)
        {
            setSkipDegree(skip_degree + 1);

            /// Still good?
            if (!good(hash))
                return;
        }

        samples.emplace_back(v, hash);
    }

    void setSkipDegree(UInt8 skip_degree_)
    {
        if (skip_degree_ == skip_degree)
            return;
        if (skip_degree_ > detail::MAX_SKIP_DEGREE)
            throw DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, "skip_degree exceeds maximum value");
        skip_degree = skip_degree_;
        if (skip_degree == detail::MAX_SKIP_DEGREE)
            skip_mask = static_cast<UInt32>(-1);
        else
            skip_mask = (1 << skip_degree) - 1;
        thinOut();
    }

    void thinOut()
    {
        samples.resize(std::distance(samples.begin(),
            std::remove_if(samples.begin(), samples.end(), [this](const auto & elem){ return !good(elem.second); })));
        sorted = false;
    }

    void sortIfNeeded()
    {
        if (sorted)
            return;

        /// In order to provide deterministic result we must sort by value and hash
        ::sort(samples.begin(), samples.end(), [](const auto & lhs, const auto & rhs) { return lhs < rhs; });
        sorted = true;
    }

    template <typename ResultType>
    ResultType onEmpty() const
    {
        if (OnEmpty == ReservoirSamplerDeterministicOnEmpty::THROW)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Quantile of empty ReservoirSamplerDeterministic");
        return NanLikeValueConstructor<ResultType, std::is_floating_point_v<ResultType>>::getValue();
    }
};

namespace DB
{
template <typename T>
void readBinary(std::pair<T, UInt32> & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}
}
