#pragma once

#include <limits>
#include <algorithm>
#include <climits>
#include <AggregateFunctions/ReservoirSampler.h>
#include <common/types.h>
#include <Common/HashTable/Hash.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
#include <Common/NaNUtils.h>
#include <Poco/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

/// Implementation of Reservoir Sampling algorithm. Incrementally selects from the added objects a random subset of the `sample_count` size.
/// Can approximately get quantiles.
/// The `quantile` call takes O(sample_count log sample_count), if after the previous call `quantile` there was at least one call to insert. Otherwise, O(1).
/// That is, it makes sense to first add, then get quantiles without adding.


namespace DB
{
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
enum class ReservoirSamplerDeterministicOnEmpty
{
    THROW,
    RETURN_NAN_OR_ZERO,
};


template <typename T,
    ReservoirSamplerDeterministicOnEmpty OnEmpty = ReservoirSamplerDeterministicOnEmpty::THROW>
class ReservoirSamplerDeterministic
{
    bool good(const UInt32 hash)
    {
        return hash == ((hash >> skip_degree) << skip_degree);
    }

public:
    ReservoirSamplerDeterministic(const size_t max_sample_size_ = detail::DEFAULT_MAX_SAMPLE_SIZE)
        : max_sample_size{max_sample_size_}
    {
    }

    void clear()
    {
        samples.clear();
        sorted = false;
        total_values = 0;
    }

    void insert(const T & v, const UInt64 determinator)
    {
        if (isNaN(v))
            return;

        const UInt32 hash = intHash64(determinator);
        if (!good(hash))
            return;

        insertImpl(v, hash);
        sorted = false;
        ++total_values;
    }

    size_t size() const
    {
        return total_values;
    }

    T quantileNearest(double level)
    {
        if (samples.empty())
            return onEmpty<T>();

        sortIfNeeded();

        double index = level * (samples.size() - 1);
        size_t int_index = static_cast<size_t>(index + 0.5);
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

        if (b.skip_degree > skip_degree)
        {
            skip_degree = b.skip_degree;
            thinOut();
        }

        for (const auto & sample : b.samples)
            if (good(sample.second))
                insertImpl(sample.first, sample.second);

        total_values += b.total_values;
    }

    void read(DB::ReadBuffer & buf)
    {
        size_t size = 0;
        DB::readIntBinary<size_t>(size, buf);
        DB::readIntBinary<size_t>(total_values, buf);

        /// Compatibility with old versions.
        if (size > total_values)
            size = total_values;

        samples.resize(size);
        for (size_t i = 0; i < size; ++i)
            DB::readPODBinary(samples[i], buf);

        sorted = false;
    }

    void write(DB::WriteBuffer & buf) const
    {
        size_t size = samples.size();
        DB::writeIntBinary<size_t>(size, buf);
        DB::writeIntBinary<size_t>(total_values, buf);

        for (size_t i = 0; i < size; ++i)
            DB::writePODBinary(samples[i], buf);
    }

private:
    /// We allocate some memory on the stack to avoid allocations when there are many objects with a small number of elements.
    using Element = std::pair<T, UInt32>;
    using Array = DB::PODArray<Element, 64>;

    const size_t max_sample_size; /// Maximum amount of stored values.
    size_t total_values = 0;   /// How many values were inserted (regardless if they remain in sample or not).
    bool sorted = false;
    Array samples;
    UInt8 skip_degree = 0;     /// The number N determining that we save only one per 2^N elements in average.

    void insertImpl(const T & v, const UInt32 hash)
    {
        /// Make a room for plus one element.
        while (samples.size() >= max_sample_size)
        {
            ++skip_degree;
            if (skip_degree > detail::MAX_SKIP_DEGREE)
                throw DB::Exception{"skip_degree exceeds maximum value", DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED};
            thinOut();
        }

        samples.emplace_back(v, hash);
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
        std::sort(samples.begin(), samples.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
        sorted = true;
    }

    template <typename ResultType>
    ResultType onEmpty() const
    {
        if (OnEmpty == ReservoirSamplerDeterministicOnEmpty::THROW)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Quantile of empty ReservoirSamplerDeterministic");
        else
            return NanLikeValueConstructor<ResultType, std::is_floating_point_v<ResultType>>::getValue();
    }
};
