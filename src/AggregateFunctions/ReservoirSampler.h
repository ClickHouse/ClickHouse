#pragma once

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <IO/Operators_pcg_random.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <base/sort.h>
#include <base/types.h>
#include <boost/multiprecision/cpp_int.hpp>
#include <pcg_random.hpp>
#include <Poco/Exception.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_LARGE_ARRAY_SIZE;
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
    static ResultType getValue() { return std::numeric_limits<ResultType>::quiet_NaN(); }
};

template <typename ResultType>
struct NanLikeValueConstructor<ResultType, false>
{
    static ResultType getValue() { return ResultType(); }
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
        /// Keep `total_values` saturated once it hits the max. A saturated state (e.g. from
        /// multiplying an aggregate state by a huge constant) can be merged into another via
        /// the insert() branch of merge(); a plain ++ would wrap SIZE_MAX to 0 and reach
        /// genRandom(0), which is UB / a debug assert failure.
        if (total_values != std::numeric_limits<size_t>::max())
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

    size_t size() const { return total_values; }

    bool empty() const { return samples.empty(); }

    T quantileNearest(double level)
    {
        if (samples.empty())
            return onEmpty<T>();

        sortIfNeeded();

        double index = level * static_cast<double>(samples.size() - 1);
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

        double index = std::max(0., std::min(static_cast<double>(samples.size() - 1), level * static_cast<double>(samples.size() - 1)));

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

        double left_coef = static_cast<double>(right_index) - index;
        double right_coef = index - static_cast<double>(left_index);

        if constexpr (DB::is_decimal<T>)
            return static_cast<double>(samples[left_index].value) * left_coef
                + static_cast<double>(samples[right_index].value) * right_coef;
        else
            return static_cast<double>(samples[left_index]) * left_coef + static_cast<double>(samples[right_index]) * right_coef;
    }

    template <typename U = T>
    U quantileInterpolatedDecimal(double level)
    requires DB::is_decimal<U>
    {
        if (samples.empty())
            return {};

        sortIfNeeded();

        const double index
            = std::max(0., std::min(static_cast<double>(samples.size() - 1), level * static_cast<double>(samples.size() - 1)));

        const size_t left_index = static_cast<size_t>(index);
        const size_t right_index = left_index + 1;
        if (right_index == samples.size())
            return samples[left_index];

        /// Keep decimal samples out of Float64. Besides rounding Int64 values near 2^63,
        /// Float64 loses distinct Decimal128 and Decimal256 values near their limits.
        /// `index` is still a Float64, so convert its fractional part to its exact binary rational
        /// representation and perform interpolation on unbounded integers.
        const double right_coef = index - static_cast<double>(left_index);
        if (right_coef == 0)
            return samples[left_index];

        UInt64 right_coef_bits{};
        std::memcpy(&right_coef_bits, &right_coef, sizeof(right_coef_bits));

        const UInt64 exponent = (right_coef_bits >> 52) & 0x7FF;
        const UInt64 mantissa = right_coef_bits & ((UInt64(1) << 52) - 1);

        using boost::multiprecision::cpp_int;
        cpp_int numerator;
        UInt64 denominator_shift{};
        if (exponent == 0)
        {
            numerator = mantissa;
            denominator_shift = 1074;
        }
        else
        {
            numerator = (UInt64(1) << 52) | mantissa;
            denominator_shift = 1075 - exponent;
        }

        const cpp_int denominator = cpp_int(1) << denominator_shift;
        const cpp_int left = toExactInteger(samples[left_index].value);
        const cpp_int right = toExactInteger(samples[right_index].value);
        const cpp_int result = (left * (denominator - numerator) + right * numerator) / denominator;

        return U(fromExactInteger<typename U::NativeType>(result));
    }

private:
    template <typename NativeType>
    static boost::multiprecision::cpp_int toExactInteger(NativeType value)
    {
        using boost::multiprecision::cpp_int;

        if constexpr (sizeof(NativeType) <= sizeof(Int64))
        {
            return cpp_int(value);
        }
        else
        {
            /// Access `items` through the logical limb order because its physical order depends on host byte order.
            cpp_int result;
            for (unsigned i = static_cast<unsigned>(std::size(value.items)); i-- > 0;)
                result = (result << 64) | value.items[NativeType::_impl::little(i)];

            if (value < NativeType{})
                result -= cpp_int(1) << (sizeof(NativeType) * 8);

            return result;
        }
    }

    template <typename NativeType>
    static NativeType fromExactInteger(boost::multiprecision::cpp_int value)
    {
        if constexpr (sizeof(NativeType) <= sizeof(Int64))
        {
            return value.convert_to<NativeType>();
        }
        else
        {
            if (value < 0)
                value += boost::multiprecision::cpp_int(1) << (sizeof(NativeType) * 8);

            NativeType result{};
            for (unsigned i = 0; i < std::size(result.items); ++i)
            {
                result.items[NativeType::_impl::little(i)] = (value & std::numeric_limits<UInt64>::max()).convert_to<UInt64>();
                value >>= 64;
            }
            return result;
        }
    }

public:
    void merge(const ReservoirSampler<T, OnEmpty> & b)
    {
        if (sample_count != b.sample_count)
            throw Poco::Exception("Cannot merge ReservoirSampler's with different sample_count");

        // There will be an aliasing issue if we merge the same object with itself. I.e. we will insert from `b.samples` into `a.samples`,
        // but both refer to the same array. It might happen in case of multiplying an aggregate function state by a numeric constant.
        // ATST, it seems that self-merging cannot improve accuracy, so there is no point to do it anyway.
        if (this == &b)
            return;

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

            /// `total_values` can overflow when an aggregate state is multiplied by a huge
            /// constant: `executeAggregateMultiply` self-merges the reservoir with
            /// exponentiation by squaring, doubling `total_values` each step. On overflow the
            /// wrapped sum would make `frequency` drop below 1, turning the loop below into a
            /// near-infinite one. Saturate the sum so `frequency` stays >= 1.
            if (__builtin_add_overflow(total_values, b.total_values, &total_values))
                total_values = std::numeric_limits<size_t>::max();

            /// Will replace every frequency'th element in a to element from b.
            double frequency = static_cast<double>(total_values) / static_cast<double>(b.total_values);

            /// When frequency is too low, replace just one random element with the corresponding probability.
            if (frequency * 2 >= static_cast<double>(sample_count))
            {
                UInt64 rnd = genRandom(static_cast<UInt64>(frequency));
                if (rnd < sample_count)
                    samples[rnd] = b.samples[rnd];
            }
            else
            {
                for (double i = 0; i < static_cast<double>(sample_count); i += frequency) /// NOLINT
                {
                    size_t idx = static_cast<size_t>(i);
                    samples[idx] = b.samples[idx];
                }
            }
        }
    }

    void read(DB::ReadBuffer & buf)
    {
        DB::readBinaryLittleEndian(sample_count, buf);
        DB::readBinaryLittleEndian(total_values, buf);

        size_t size = std::min(total_values, sample_count);
        static constexpr size_t MAX_RESERVOIR_SIZE = 1_GiB;
        if (unlikely(size > MAX_RESERVOIR_SIZE))
            throw DB::Exception(DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size (maximum: {})", MAX_RESERVOIR_SIZE);

        samples.resize(size);

        std::string rng_string;
        DB::readStringBinary(rng_string, buf);
        DB::ReadBufferFromString rng_buf(rng_string);
        rng_buf >> rng;

        for (size_t i = 0; i < samples.size(); ++i)
            DB::readBinaryLittleEndian(samples[i], buf);

        sorted = false;
    }

    void write(DB::WriteBuffer & buf) const
    {
        DB::writeBinaryLittleEndian(sample_count, buf);
        DB::writeBinaryLittleEndian(total_values, buf);

        DB::WriteBufferFromOwnString rng_buf;
        rng_buf << rng;
        DB::writeStringBinary(rng_buf.str(), buf);

        for (size_t i = 0; i < std::min(sample_count, total_values); ++i)
            DB::writeBinaryLittleEndian(samples[i], buf);
    }

private:
    /// We allocate a little memory on the stack - to avoid allocations when there are many objects with a small number of elements.
    using Array = DB::PODArrayWithStackMemory<T, 64>;

    size_t sample_count;
    size_t total_values = 0;
    Array samples;
    pcg32_fast rng;
    bool sorted = false;

    UInt64 genRandom(UInt64 limit)
    {
        chassert(limit > 0);

        /// With a large number of values, we will generate random numbers several times slower.
        if (limit <= static_cast<UInt64>(pcg32_fast::max()))
            return rng() % limit; /// NOLINT(clang-analyzer-core.DivideZero)
        return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(pcg32_fast::max()) + 1ULL) + static_cast<UInt64>(rng())) % limit;
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
        return NanLikeValueConstructor<ResultType, is_floating_point<ResultType>>::getValue();
    }
};
