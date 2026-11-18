#pragma once

#include <algorithm>
#include <array>
#include <cmath>
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
        next_selected = 0;
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

    /// The same as `insert` for each of `count` values, but from position `SKIP_AHEAD_FROM * sample_count` on
    /// it draws the position of the next kept value instead of a random number for every value.
    void insertMany(const T * __restrict values, size_t count)
    {
        if constexpr (is_floating_point<T>)
        {
            /// Not `std::any_of`: a loop without an early exit is vectorized.
            bool has_nan = false;
            for (size_t i = 0; i < count; ++i)
                has_nan |= isNaN(values[i]);
            if (has_nan)
            {
                for (size_t i = 0; i < count; ++i)
                    insert(values[i]);
                return;
            }
        }
        insertManyImpl(count, [values](size_t i) -> const T & { return values[i]; });
    }

    /// The same as `insert(v)` `count` times, see `insertMany`.
    void insertManySame(const T & v, size_t count)
    {
        if (isNaN(v))
            return;
        insertManyImpl(count, [&v](size_t) -> const T & { return v; });
    }

    /// The same as `insert` for each of `count` values where `(mask[i] != 0) == keep_nonzero`, see `insertMany`.
    template <bool keep_nonzero>
    void insertManyFiltered(const T * __restrict values, size_t count, const UInt8 * __restrict mask)
    {
        size_t i = 0;
        if (total_values < skipAheadFrom())
        {
            for (; i < count; ++i)
            {
                if ((mask[i] != 0) == keep_nonzero)
                {
                    insert(values[i]);
                    if (total_values >= skipAheadFrom())
                    {
                        ++i;
                        break;
                    }
                }
            }
        }

        static constexpr size_t CHUNK = 64;
        while (i < count)
        {
            if (unlikely(total_values >= MAX_NEXT_SELECTED - CHUNK))
            {
                for (; i < count; ++i)
                    if ((mask[i] != 0) == keep_nonzero)
                        insert(values[i]);
                return;
            }
            if (next_selected <= total_values)
                next_selected = drawNextSelected(total_values);

            /// Count the values of a chunk without branches, and skip it if the next selected value is not there.
            const size_t end = std::min(count, i + CHUNK);
            size_t in_chunk = 0;
            for (size_t j = i; j < end; ++j)
                in_chunk += ((mask[j] != 0) == keep_nonzero) & !isNaN(values[j]);
            if (in_chunk < next_selected - total_values)
            {
                total_values += in_chunk;
                i = end;
                continue;
            }

            for (; i < end; ++i)
            {
                total_values += ((mask[i] != 0) == keep_nonzero) & !isNaN(values[i]);
                if (total_values == next_selected)
                {
                    insertSelected(values[i]);
                    ++i;
                    break;
                }
            }
        }
    }

    [[nodiscard]] size_t size() const { return total_values; }

    [[nodiscard]] bool empty() const { return samples.empty(); }

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
        next_selected = 0;

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
        next_selected = 0;
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

    /// `insertMany` draws the position of the next kept value from position `SKIP_AHEAD_FROM * sample_count` on.
    /// Before that many values are kept anyway, and the results are the same as with a random number for every value.
    static constexpr size_t SKIP_AHEAD_FROM = 16;

    size_t sample_count;
    size_t total_values = 0;
    Array samples;
    pcg32_fast rng;
    bool sorted = false;
    /// The position of the next value that `insertMany` keeps, or 0. Not serialized: it only depends on random draws
    /// and on `total_values`. A position that `insert` has passed is drawn again, and one that it has not passed
    /// is still valid, because the values at different positions are kept independently.
    /// A bit field in the padding after `sorted`, so that the state does not grow.
    size_t next_selected : 56 = 0;
    static constexpr size_t MAX_NEXT_SELECTED = (size_t(1) << 56) - 1;

    UInt64 genRandom(UInt64 limit)
    {
        chassert(limit > 0);

        /// With a large number of values, we will generate random numbers several times slower.
        if (limit <= static_cast<UInt64>(pcg32_fast::max()))
            return rng() % limit; /// NOLINT(clang-analyzer-core.DivideZero)
        return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(pcg32_fast::max()) + 1ULL) + static_cast<UInt64>(rng())) % limit;
    }

    [[nodiscard]] size_t skipAheadFrom() const
    {
        return sample_count > std::numeric_limits<size_t>::max() / SKIP_AHEAD_FROM ? std::numeric_limits<size_t>::max()
                                                                                : sample_count * SKIP_AHEAD_FROM;
    }

    /// `get(i)` returns the value at index `i` of `count` values without NaN.
    template <typename Get>
    void insertManyImpl(size_t count, Get && get)
    {
        size_t i = 0;
        if (total_values < skipAheadFrom())
            for (const size_t per_value = std::min(count, skipAheadFrom() - total_values); i < per_value; ++i)
                insert(get(i));

        while (i < count)
        {
            if (unlikely(total_values >= MAX_NEXT_SELECTED - (count - i)))
            {
                for (; i < count; ++i)
                    insert(get(i));
                return;
            }
            if (next_selected <= total_values)
                next_selected = drawNextSelected(total_values);

            /// The value at index `i` has position `total_values + 1`.
            if (next_selected - total_values > count - i)
            {
                total_values += count - i;
                return;
            }

            i += next_selected - total_values;
            total_values = next_selected;
            insertSelected(get(i - 1));
        }
    }

    /// A uniform random number in [0, 1) with 53 random bits.
    double genUniform()
    {
        const UInt64 high = rng();
        const UInt64 low = rng();
        return static_cast<double>((high << 21) | (low >> 11)) * 0x1p-53;
    }

    /// Keeps `v` at position `total_values` == `next_selected` and draws the next position.
    void insertSelected(const T & v)
    {
        sorted = false;
        samples[genRandom(sample_count)] = v;
        next_selected = drawNextSelected(total_values);
    }

    /// Returns the position of the next value kept by Algorithm R after `seen` >= `sample_count` values,
    /// or MAX_NEXT_SELECTED if it is beyond that. Algorithm R keeps the value at position i independently
    /// with probability sample_count / i. Candidates are drawn from the geometric distribution with the
    /// probability for position `seen + 1`, which is not less than for the later positions, and a candidate
    /// at position i is accepted with probability (seen + 1) / i (thinning). The result has exactly the
    /// distribution of drawing for each value, while a candidate is rejected with probability below
    /// 1 / sample_count. This is the idea of J. S. Vitter, "Random sampling with a reservoir" (1985).
    size_t drawNextSelected(size_t seen)
    {
        static constexpr size_t max = MAX_NEXT_SELECTED;
        const double count = static_cast<double>(sample_count);
        while (true)
        {
            const double first = static_cast<double>(seen) + 1;
            /// The number of skipped candidates: P(gap >= g) = (1 - count / first)^g.
            const double gap = std::floor(std::log(1 - genUniform()) / std::log1p(-count / first));
            if (!(gap >= 0 && gap < 0x1p63))
                return max;
            const size_t step = static_cast<size_t>(gap) + 1;
            if (step > max - seen)
                return max;
            seen += step;
            if (genUniform() * static_cast<double>(seen) < first)
                return seen;
        }
    }

    void sortIfNeeded()
    {
        if (sorted)
            return;
        sorted = true;
        ::sort(samples.begin(), samples.end(), Comparer());
    }

    template <typename ResultType>
    [[nodiscard]] ResultType onEmpty() const
    {
        if (OnEmpty == ReservoirSamplerOnEmpty::THROW)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Quantile of empty ReservoirSampler");
        return NanLikeValueConstructor<ResultType, is_floating_point<ResultType>>::getValue();
    }
};
