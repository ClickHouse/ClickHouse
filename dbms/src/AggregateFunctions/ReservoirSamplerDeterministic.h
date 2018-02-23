#pragma once

#include <limits>
#include <algorithm>
#include <climits>
#include <sstream>
#include <AggregateFunctions/ReservoirSampler.h>
#include <common/Types.h>
#include <Common/HashTable/Hash.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
#include <Poco/Exception.h>


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
const size_t DEFAULT_SAMPLE_COUNT = 8192;
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
    ReservoirSamplerDeterministic(const size_t sample_count = DEFAULT_SAMPLE_COUNT)
        : sample_count{sample_count}
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
            return samples[left_index].first;

        const double left_coef = right_index - index;
        const double right_coef = index - left_index;

        return samples[left_index].first * left_coef + samples[right_index].first * right_coef;
    }

    void merge(const ReservoirSamplerDeterministic & b)
    {
        if (sample_count != b.sample_count)
            throw Poco::Exception("Cannot merge ReservoirSamplerDeterministic's with different sample_count");
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
        DB::readIntBinary<size_t>(sample_count, buf);
        DB::readIntBinary<size_t>(total_values, buf);
        samples.resize(std::min(total_values, sample_count));

        for (size_t i = 0; i < samples.size(); ++i)
            DB::readPODBinary(samples[i], buf);

        sorted = false;
    }

    void write(DB::WriteBuffer & buf) const
    {
        DB::writeIntBinary<size_t>(sample_count, buf);
        DB::writeIntBinary<size_t>(total_values, buf);

        for (size_t i = 0; i < std::min(sample_count, total_values); ++i)
            DB::writePODBinary(samples[i], buf);
    }

private:
    /// We allocate some memory on the stack to avoid allocations when there are many objects with a small number of elements.
    static constexpr size_t bytes_on_stack = 64;
    using Element = std::pair<T, UInt32>;
    using Array = DB::PODArray<Element, bytes_on_stack / sizeof(Element), AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;

    size_t sample_count;
    size_t total_values{};
    bool sorted{};
    Array samples;
    UInt8 skip_degree{};

    void insertImpl(const T & v, const UInt32 hash)
    {
        /// @todo why + 1? I don't quite recall
        while (samples.size() + 1 >= sample_count)
        {
            if (++skip_degree > detail::MAX_SKIP_DEGREE)
                throw DB::Exception{"skip_degree exceeds maximum value", DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED};
            thinOut();
        }

        samples.emplace_back(v, hash);
    }

    void thinOut()
    {
        auto size = samples.size();
        for (size_t i = 0; i < size;)
        {
            if (!good(samples[i].second))
            {
                /// swap current element with the last one
                std::swap(samples[size - 1], samples[i]);
                --size;
            }
            else
                ++i;
        }

        if (size != samples.size())
        {
            samples.resize(size);
            sorted = false;
        }
    }

    void sortIfNeeded()
    {
        if (sorted)
            return;
        sorted = true;
        std::sort(samples.begin(), samples.end(), [] (const std::pair<T, UInt32> & lhs, const std::pair<T, UInt32> & rhs)
        {
            return lhs.first < rhs.first;
        });
    }

    template <typename ResultType>
    ResultType onEmpty() const
    {
        if (OnEmpty == ReservoirSamplerDeterministicOnEmpty::THROW)
            throw Poco::Exception("Quantile of empty ReservoirSamplerDeterministic");
        else
            return NanLikeValueConstructor<ResultType, std::is_floating_point_v<ResultType>>::getValue();
    }
};
