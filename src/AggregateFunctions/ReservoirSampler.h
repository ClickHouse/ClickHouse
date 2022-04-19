#pragma once

#include <limits>
#include <algorithm>
#include <climits>
#include <base/types.h>
#include <base/sort.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/PODArray.h>
#include <Common/NaNUtils.h>
#include <Poco/Exception.h>
#include <pcg_random.hpp>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

/// Implementing the Reservoir Sampling algorithm. Incrementally selects from the added objects a random subset of the sample_count size.
/// Can approximately get quantiles.
/// Call `quantile` takes O(sample_count log sample_count), if after the previous call `quantile` there was at least one call `insert`. Otherwise O(1).
/// That is, it makes sense to first add, then get quantiles without adding.

const size_t DEFAULT_SAMPLE_COUNT = 8192;

/// What if there is not a single value - throw an exception, or return 0 or NaN in the case of double?
namespace ReservoirSamplerOnEmpty
{
    enum Enum
    {
        THROW,
        RETURN_NAN_OR_ZERO,
    };
}

template <typename ResultType, bool is_float>
struct NanLikeValueConstructor
{
    static ResultType getValue()
    {
        return std::numeric_limits<ResultType>::quiet_NaN();
    }
};
template <typename ResultType>
struct NanLikeValueConstructor<ResultType, false>
{
    static ResultType getValue()
    {
        return ResultType();
    }
};

template <typename T, ReservoirSamplerOnEmpty::Enum OnEmpty = ReservoirSamplerOnEmpty::THROW, typename Comparer = std::less<T>>
class ReservoirSampler
{
public:
    explicit ReservoirSampler(size_t sample_count_ = DEFAULT_SAMPLE_COUNT)
        : sample_count(sample_count_)
    {
        rng.seed(123456);
    }

    void clear()
    {
        samples.clear();
        sorted = false;
        total_values = 0;
        rng.seed(123456);
    }

    void insert(const T & v)
    {
        if (isNaN(v))
            return;

        sorted = false;
        ++total_values;
        if (samples.size() < sample_count)
        {
            samples.push_back(v);
        }
        else
        {
            UInt64 rnd = genRandom(total_values);
            if (rnd < sample_count)
                samples[rnd] = v;
        }
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
        size_t int_index = static_cast<size_t>(index + 0.5); /// NOLINT
        int_index = std::max(0LU, std::min(samples.size() - 1, int_index));
        return samples[int_index];
    }

    /** If T is not a numeric type, using this method causes a compilation error,
      *  but use of error class does not. SFINAE.
      */
    double quantileInterpolated(double level)
    {
        if (samples.empty())
        {
            if (DB::is_decimal<T>)
                return 0;
            return onEmpty<double>();
        }
        sortIfNeeded();

        double index = std::max(0., std::min(samples.size() - 1., level * (samples.size() - 1)));

        /// To get the value of a fractional index, we linearly interpolate between neighboring values.
        size_t left_index = static_cast<size_t>(index);
        size_t right_index = left_index + 1;
        if (right_index == samples.size())
        {
            if constexpr (DB::is_decimal<T>)
                return static_cast<double>(samples[left_index].value);
            else
                return static_cast<double>(samples[left_index]);
        }

        double left_coef = right_index - index;
        double right_coef = index - left_index;

        if constexpr (DB::is_decimal<T>)
            return static_cast<double>(samples[left_index].value) * left_coef + static_cast<double>(samples[right_index].value) * right_coef;
        else
            return static_cast<double>(samples[left_index]) * left_coef + static_cast<double>(samples[right_index]) * right_coef;
    }

    void merge(const ReservoirSampler<T, OnEmpty> & b)
    {
        if (sample_count != b.sample_count)
            throw Poco::Exception("Cannot merge ReservoirSampler's with different sample_count");
        sorted = false;

        if (b.total_values <= sample_count)
        {
            for (size_t i = 0; i < b.samples.size(); ++i)
                insert(b.samples[i]);
        }
        else if (total_values <= sample_count)
        {
            Array from = std::move(samples);
            samples.assign(b.samples.begin(), b.samples.end());
            total_values = b.total_values;
            for (size_t i = 0; i < from.size(); ++i)
                insert(from[i]);
        }
        else
        {
            /// Replace every element in our reservoir to the b's reservoir
            /// with the probability of b.total_values / (a.total_values + b.total_values)
            /// Do it more roughly than true random sampling to save performance.

            total_values += b.total_values;

            /// Will replace every frequency'th element in a to element from b.
            double frequency = static_cast<double>(total_values) / b.total_values;

            /// When frequency is too low, replace just one random element with the corresponding probability.
            if (frequency * 2 >= sample_count)
            {
                UInt64 rnd = genRandom(frequency);
                if (rnd < sample_count)
                    samples[rnd] = b.samples[rnd];
            }
            else
            {
                for (double i = 0; i < sample_count; i += frequency) /// NOLINT
                    samples[i] = b.samples[i];
            }
        }
    }

    void read(DB::ReadBuffer & buf)
    {
        DB::readIntBinary<size_t>(sample_count, buf);
        DB::readIntBinary<size_t>(total_values, buf);
        samples.resize(std::min(total_values, sample_count));

        std::string rng_string;
        DB::readStringBinary(rng_string, buf);
        DB::ReadBufferFromString rng_buf(rng_string);
        rng_buf >> rng;

        for (size_t i = 0; i < samples.size(); ++i)
            DB::readBinary(samples[i], buf);

        sorted = false;
    }

    void write(DB::WriteBuffer & buf) const
    {
        DB::writeIntBinary<size_t>(sample_count, buf);
        DB::writeIntBinary<size_t>(total_values, buf);

        DB::WriteBufferFromOwnString rng_buf;
        rng_buf << rng;
        DB::writeStringBinary(rng_buf.str(), buf);

        for (size_t i = 0; i < std::min(sample_count, total_values); ++i)
            DB::writeBinary(samples[i], buf);
    }

private:
    /// We allocate a little memory on the stack - to avoid allocations when there are many objects with a small number of elements.
    using Array = DB::PODArrayWithStackMemory<T, 64>;

    size_t sample_count;
    size_t total_values = 0;
    Array samples;
    pcg32_fast rng;
    bool sorted = false;


    UInt64 genRandom(size_t lim)
    {
        assert(lim > 0);
        /// With a large number of values, we will generate random numbers several times slower.
        if (lim <= static_cast<UInt64>(rng.max()))
            return static_cast<UInt32>(rng()) % static_cast<UInt32>(lim);
        else
            return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(rng.max()) + 1ULL) + static_cast<UInt64>(rng())) % lim;
    }

    void sortIfNeeded()
    {
        if (sorted)
            return;
        sorted = true;
        ::sort(samples.begin(), samples.end(), Comparer());
    }

    template <typename ResultType>
    ResultType onEmpty() const
    {
        if (OnEmpty == ReservoirSamplerOnEmpty::THROW)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Quantile of empty ReservoirSampler");
        else
            return NanLikeValueConstructor<ResultType, std::is_floating_point_v<ResultType>>::getValue();
    }
};
