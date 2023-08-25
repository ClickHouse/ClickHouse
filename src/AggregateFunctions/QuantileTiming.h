#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <base/sort.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Calculates quantile for time in milliseconds, less than 30 seconds.
  * If the value is greater than 30 seconds, the value is set to 30 seconds.
  *
  * If total values is not greater than about 5670, then the calculation is accurate.
  *
  * Otherwise
  *  If time less that 1024 ms, than calculation is accurate.
  *  Otherwise, the computation is rounded to a multiple of 16 ms.
  *
  * Three different data structures are used:
  * - flat array (of all met values) of fixed length, allocated inplace, size 64 bytes; Stores 0..31 values;
  * - flat array (of all values encountered), allocated separately, increasing length;
  * - a histogram (that is, value -> number), consisting of two parts
  * -- for values from 0 to 1023 - in increments of 1;
  * -- for values from 1024 to 30,000 - in increments of 16;
  *
  * NOTE: 64-bit integer weight can overflow, see also QantileExactWeighted.h::get()
  */

#define TINY_MAX_ELEMS 31
#define BIG_THRESHOLD 30000

namespace detail
{
    /** Helper structure for optimization in the case of a small number of values
      * - flat array of a fixed size "on the stack" in which all encountered values placed in succession.
      * Size - 64 bytes. Must be a POD-type (used in union).
      */
    struct QuantileTimingTiny
    {
        mutable UInt16 elems[TINY_MAX_ELEMS];    /// mutable because array sorting is not considered a state change.
        /// It's important that `count` be at the end of the structure, since the beginning of the structure will be subsequently rewritten by other objects.
        /// You must initialize it by zero itself.
        /// Why? `count` field is reused even in cases where the union contains other structures
        ///  (the size of which falls short of this field.)
        UInt16 count;

        /// Can only be used while `count < TINY_MAX_ELEMS`.
        void insert(UInt64 x)
        {
            if (unlikely(x > BIG_THRESHOLD))
                x = BIG_THRESHOLD;

            elems[count] = x;
            ++count;
        }

        /// Can only be used while `count + rhs.count <= TINY_MAX_ELEMS`.
        void merge(const QuantileTimingTiny & rhs)
        {
            for (size_t i = 0; i < rhs.count; ++i)
            {
                elems[count] = rhs.elems[i];
                ++count;
            }
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinary(count, buf);
            buf.write(reinterpret_cast<const char *>(elems), count * sizeof(elems[0]));
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinary(count, buf);
            buf.readStrict(reinterpret_cast<char *>(elems), count * sizeof(elems[0]));
        }

        /** This function must be called before get-functions. */
        void prepare() const
        {
            ::sort(elems, elems + count);
        }

        UInt16 get(double level) const
        {
            return level != 1
                ? elems[static_cast<size_t>(count * level)]
                : elems[count - 1];
        }

        template <typename ResultType>
        void getMany(const double * levels, size_t size, ResultType * result) const
        {
            const double * levels_end = levels + size;

            while (levels != levels_end)
            {
                *result = get(*levels);
                ++levels;
                ++result;
            }
        }

        /// The same, but in the case of an empty state NaN is returned.
        float getFloat(double level) const
        {
            return count
                ? get(level)
                : std::numeric_limits<float>::quiet_NaN();
        }

        void getManyFloat(const double * levels, size_t size, float * result) const
        {
            if (count)
                getMany(levels, size, result);
            else
                for (size_t i = 0; i < size; ++i)
                    result[i] = std::numeric_limits<float>::quiet_NaN();
        }
    };


    /** Auxiliary structure for optimization in case of average number of values
      *  - a flat array, allocated separately, into which all found values are put in succession.
      */
    struct QuantileTimingMedium
    {
        /// sizeof - 24 bytes.
        using Array = PODArray<UInt16, 128>;
        mutable Array elems;    /// mutable because array sorting is not considered a state change.

        QuantileTimingMedium() = default;
        QuantileTimingMedium(const UInt16 * begin, const UInt16 * end) : elems(begin, end) {}

        void insert(UInt64 x)
        {
            if (unlikely(x > BIG_THRESHOLD))
                x = BIG_THRESHOLD;

            elems.emplace_back(x);
        }

        void merge(const QuantileTimingMedium & rhs)
        {
            elems.insert(rhs.elems.begin(), rhs.elems.end());
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinary(elems.size(), buf);
            buf.write(reinterpret_cast<const char *>(elems.data()), elems.size() * sizeof(elems[0]));
        }

        void deserialize(ReadBuffer & buf)
        {
            size_t size = 0;
            readBinary(size, buf);
            elems.resize(size);
            buf.readStrict(reinterpret_cast<char *>(elems.data()), size * sizeof(elems[0]));
        }

        UInt16 get(double level)
        {
            UInt16 quantile = 0;

            if (!elems.empty())
            {
                size_t n = level < 1
                    ? level * elems.size()
                    : (elems.size() - 1);

                /// Sorting an array will not be considered a violation of constancy.
                auto & array = elems;
                ::nth_element(array.begin(), array.begin() + n, array.end());
                quantile = array[n];
            }

            return quantile;
        }

        template <typename ResultType>
        void getMany(const double * levels, const size_t * levels_permutation, size_t size, ResultType * result)
        {
            size_t prev_n = 0;
            auto & array = elems;
            for (size_t i = 0; i < size; ++i)
            {
                auto level_index = levels_permutation[i];
                auto level = levels[level_index];

                size_t n = level < 1
                    ? level * elems.size()
                    : (elems.size() - 1);

                ::nth_element(array.begin() + prev_n, array.begin() + n, array.end());

                result[level_index] = array[n];
                prev_n = n;
            }
        }

        /// Same, but in the case of an empty state, NaN is returned.
        float getFloat(double level)
        {
            return !elems.empty()
                ? get(level)
                : std::numeric_limits<float>::quiet_NaN();
        }

        void getManyFloat(const double * levels, const size_t * levels_permutation, size_t size, float * result)
        {
            if (!elems.empty())
                getMany(levels, levels_permutation, size, result);
            else
                for (size_t i = 0; i < size; ++i)
                    result[i] = std::numeric_limits<float>::quiet_NaN();
        }
    };


    #define SMALL_THRESHOLD 1024
    #define BIG_SIZE ((BIG_THRESHOLD - SMALL_THRESHOLD) / BIG_PRECISION)
    #define BIG_PRECISION 16

    #define SIZE_OF_LARGE_WITHOUT_COUNT ((SMALL_THRESHOLD + BIG_SIZE) * sizeof(UInt64))


    /** For a large number of values. The size is about 22 680 bytes.
      */
    class QuantileTimingLarge
    {
    private:
        /// Total number of values.
        UInt64 count;
        /// Use of UInt64 is very wasteful.
        /// But UInt32 is definitely not enough, and it's too hard to invent 6-byte values.

        /// Number of values for each value is smaller than `small_threshold`.
        UInt64 count_small[SMALL_THRESHOLD];

        /// The number of values for each value from `small_threshold` to `big_threshold`, rounded to `big_precision`.
        UInt64 count_big[BIG_SIZE];

        /// Get value of quantile by index in array `count_big`.
        static inline UInt16 indexInBigToValue(size_t i)
        {
            return (i * BIG_PRECISION) + SMALL_THRESHOLD
                + (intHash32<0>(i) % BIG_PRECISION - (BIG_PRECISION / 2));    /// A small randomization so that it is not noticeable that all the values are even.
        }

        /// Lets you scroll through the histogram values, skipping zeros.
        class Iterator
        {
        private:
            const UInt64 * begin;
            const UInt64 * pos;
            const UInt64 * end;

            void adjust()
            {
                while (isValid() && 0 == *pos)
                    ++pos;
            }

        public:
            explicit Iterator(const QuantileTimingLarge & parent)
                : begin(parent.count_small), pos(begin), end(&parent.count_big[BIG_SIZE])
            {
                adjust();
            }

            bool isValid() const { return pos < end; }

            void next()
            {
                ++pos;
                adjust();
            }

            UInt64 count() const { return *pos; }

            UInt16 key() const
            {
                return pos - begin < SMALL_THRESHOLD
                    ? pos - begin
                    : indexInBigToValue(pos - begin - SMALL_THRESHOLD);
            }
        };

    public:
        QuantileTimingLarge()
        {
            memset(this, 0, sizeof(*this));
        }

        void insert(UInt64 x) noexcept
        {
            insertWeighted(x, 1);
        }

        void insertWeighted(UInt64 x, size_t weight) noexcept
        {
            count += weight;

            if (x < SMALL_THRESHOLD)
                count_small[x] += weight;
            else if (x < BIG_THRESHOLD)
                count_big[(x - SMALL_THRESHOLD) / BIG_PRECISION] += weight;
        }

        void merge(const QuantileTimingLarge & rhs) noexcept
        {
            count += rhs.count;

            for (size_t i = 0; i < SMALL_THRESHOLD; ++i)
                count_small[i] += rhs.count_small[i];

            for (size_t i = 0; i < BIG_SIZE; ++i)
                count_big[i] += rhs.count_big[i];
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinary(count, buf);

            if (count * 2 > SMALL_THRESHOLD + BIG_SIZE)
            {
                /// Simple serialization for a heavily dense case.
                buf.write(reinterpret_cast<const char *>(this) + sizeof(count), SIZE_OF_LARGE_WITHOUT_COUNT);
            }
            else
            {
                /// More compact serialization for a sparse case.

                for (size_t i = 0; i < SMALL_THRESHOLD; ++i)
                {
                    if (count_small[i])
                    {
                        writeBinary(UInt16(i), buf);
                        writeBinary(count_small[i], buf);
                    }
                }

                for (size_t i = 0; i < BIG_SIZE; ++i)
                {
                    if (count_big[i])
                    {
                        writeBinary(UInt16(i + SMALL_THRESHOLD), buf);
                        writeBinary(count_big[i], buf);
                    }
                }

                /// Symbolizes end of data.
                writeBinary(UInt16(BIG_THRESHOLD), buf);
            }
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinary(count, buf);

            if (count * 2 > SMALL_THRESHOLD + BIG_SIZE)
            {
                buf.readStrict(reinterpret_cast<char *>(this) + sizeof(count), SIZE_OF_LARGE_WITHOUT_COUNT);
            }
            else
            {
                while (true)
                {
                    UInt16 index = 0;
                    readBinary(index, buf);
                    if (index == BIG_THRESHOLD)
                        break;

                    UInt64 elem_count = 0;
                    readBinary(elem_count, buf);

                    if (index < SMALL_THRESHOLD)
                        count_small[index] = elem_count;
                    else
                        count_big[index - SMALL_THRESHOLD] = elem_count;
                }
            }
        }


        /// Get the value of the `level` quantile. The level must be between 0 and 1.
        UInt16 get(double level) const
        {
            double pos = std::ceil(count * level);

            double accumulated = 0;
            Iterator it(*this);

            while (it.isValid())
            {
                accumulated += it.count();

                if (accumulated >= pos)
                    break;

                it.next();
            }

            return it.isValid() ? it.key() : BIG_THRESHOLD;
        }

        /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
        /// indices - an array of index levels such that the corresponding elements will go in ascending order.
        template <typename ResultType>
        void getMany(const double * levels, const size_t * indices, size_t size, ResultType * result) const
        {
            const auto * indices_end = indices + size;
            const auto * index = indices;

            double pos = std::ceil(count * levels[*index]);

            double accumulated = 0;
            Iterator it(*this);

            while (it.isValid())
            {
                accumulated += it.count();

                while (accumulated >= pos)
                {
                    result[*index] = it.key();
                    ++index;

                    if (index == indices_end)
                        return;

                    pos = std::ceil(count * levels[*index]);
                }

                it.next();
            }

            while (index != indices_end)
            {
                result[*index] = std::numeric_limits<ResultType>::max() < BIG_THRESHOLD
                    ? std::numeric_limits<ResultType>::max() : BIG_THRESHOLD;
                ++index;
            }
        }

        /// The same, but in the case of an empty state, NaN is returned.
        float getFloat(double level) const
        {
            return count
                ? get(level)
                : std::numeric_limits<float>::quiet_NaN();
        }

        void getManyFloat(const double * levels, const size_t * levels_permutation, size_t size, float * result) const
        {
            if (count)
                getMany(levels, levels_permutation, size, result);
            else
                for (size_t i = 0; i < size; ++i)
                    result[i] = std::numeric_limits<float>::quiet_NaN();
        }
    };
}


/** sizeof - 64 bytes.
  * If there are not enough of them - allocates up to 20 KB of memory in addition.
  */
template <typename>     /// Unused template parameter is for AggregateFunctionQuantile.
class QuantileTiming : private boost::noncopyable
{
private:
    union
    {
        detail::QuantileTimingTiny tiny;
        detail::QuantileTimingMedium medium;
        detail::QuantileTimingLarge * large;
    };

    enum class Kind : uint8_t
    {
        Tiny = 1,
        Medium = 2,
        Large = 3
    };

    Kind which() const
    {
        if (tiny.count <= TINY_MAX_ELEMS)
            return Kind::Tiny;
        if (tiny.count == TINY_MAX_ELEMS + 1)
            return Kind::Medium;
        return Kind::Large;
    }

    void tinyToMedium()
    {
        detail::QuantileTimingTiny tiny_copy = tiny;
        new (&medium) detail::QuantileTimingMedium(tiny_copy.elems, tiny_copy.elems + tiny_copy.count);
        tiny.count = TINY_MAX_ELEMS + 1;
    }

    void mediumToLarge()
    {
        /// While the data is copied from medium, it is not possible to set `large` value (otherwise it will overwrite some data).
        detail::QuantileTimingLarge * tmp_large = new detail::QuantileTimingLarge;

        for (const auto & elem : medium.elems)
            tmp_large->insert(elem);    /// Cannot throw, so don't worry about new.

        medium.~QuantileTimingMedium();
        large = tmp_large;
        tiny.count = TINY_MAX_ELEMS + 2;    /// large will be deleted in destructor.
    }

    void tinyToLarge()
    {
        /// While the data is copied from `medium` it is not possible to set `large` value (otherwise it will overwrite some data).
        detail::QuantileTimingLarge * tmp_large = new detail::QuantileTimingLarge;

        for (size_t i = 0; i < tiny.count; ++i)
            tmp_large->insert(tiny.elems[i]);    /// Cannot throw, so don't worry about new.

        large = tmp_large;
        tiny.count = TINY_MAX_ELEMS + 2;    /// large will be deleted in destructor.
    }

    bool mediumIsWorthToConvertToLarge() const
    {
        return medium.elems.size() >= sizeof(detail::QuantileTimingLarge) / sizeof(medium.elems[0]) / 2;
    }

public:
    QuantileTiming()
    {
        tiny.count = 0;
    }

    ~QuantileTiming()
    {
        Kind kind = which();

        if (kind == Kind::Medium)
        {
            medium.~QuantileTimingMedium();
        }
        else if (kind == Kind::Large)
        {
            delete large;
        }
    }

    void add(UInt64 x)
    {
        if (tiny.count < TINY_MAX_ELEMS)
        {
            tiny.insert(x);
        }
        else
        {
            if (unlikely(tiny.count == TINY_MAX_ELEMS))
                tinyToMedium();

            if (which() == Kind::Medium)
            {
                if (unlikely(mediumIsWorthToConvertToLarge()))
                {
                    mediumToLarge();
                    large->insert(x);
                }
                else
                    medium.insert(x);
            }
            else
                large->insert(x);
        }
    }

    void add(UInt64 x, size_t weight)
    {
        /// NOTE: First condition is to avoid overflow.
        if (weight < TINY_MAX_ELEMS && tiny.count + weight <= TINY_MAX_ELEMS)
        {
            for (size_t i = 0; i < weight; ++i)
                tiny.insert(x);
        }
        else
        {
            if (unlikely(tiny.count <= TINY_MAX_ELEMS))
                tinyToLarge();    /// For the weighted variant we do not use `medium` - presumably, it is impractical.

            large->insertWeighted(x, weight);
        }
    }

    /// NOTE Too complicated.
    void merge(const QuantileTiming & rhs)
    {
        if (tiny.count + rhs.tiny.count <= TINY_MAX_ELEMS)
        {
            tiny.merge(rhs.tiny);
        }
        else
        {
            auto kind = which();
            auto rhs_kind = rhs.which();

            /// If one with which we merge has a larger data structure, then we bring the current structure to the same one.
            if (kind == Kind::Tiny && rhs_kind == Kind::Medium)
            {
                tinyToMedium();
                kind = Kind::Medium;
            }
            else if (kind == Kind::Tiny && rhs_kind == Kind::Large)
            {
                tinyToLarge();
                kind = Kind::Large;
            }
            else if (kind == Kind::Medium && rhs_kind == Kind::Large)
            {
                mediumToLarge();
                kind = Kind::Large;
            }
            /// Case when two states are small, but when merged, they will turn into average.
            else if (kind == Kind::Tiny && rhs_kind == Kind::Tiny)
            {
                tinyToMedium();
                kind = Kind::Medium;
            }

            if (kind == Kind::Medium && rhs_kind == Kind::Medium)
            {
                medium.merge(rhs.medium);
            }
            else if (kind == Kind::Large && rhs_kind == Kind::Large)
            {
                large->merge(*rhs.large);
            }
            else if (kind == Kind::Medium && rhs_kind == Kind::Tiny)
            {
                medium.elems.insert(rhs.tiny.elems, rhs.tiny.elems + rhs.tiny.count);
            }
            else if (kind == Kind::Large && rhs_kind == Kind::Tiny)
            {
                for (size_t i = 0; i < rhs.tiny.count; ++i)
                    large->insert(rhs.tiny.elems[i]);
            }
            else if (kind == Kind::Large && rhs_kind == Kind::Medium)
            {
                for (const auto & elem : rhs.medium.elems)
                    large->insert(elem);
            }
            else
                throw Exception("Logical error in QuantileTiming::merge function: not all cases are covered", ErrorCodes::LOGICAL_ERROR);

            /// For determinism, we should always convert to `large` when size condition is reached
            ///  - regardless of merge order.
            if (kind == Kind::Medium && unlikely(mediumIsWorthToConvertToLarge()))
            {
                mediumToLarge();
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        auto kind = which();
        DB::writePODBinary(kind, buf);

        if (kind == Kind::Tiny)
            tiny.serialize(buf);
        else if (kind == Kind::Medium)
            medium.serialize(buf);
        else
            large->serialize(buf);
    }

    /// Called for an empty object.
    void deserialize(ReadBuffer & buf)
    {
        Kind kind;
        DB::readPODBinary(kind, buf);

        if (kind == Kind::Tiny)
        {
            tiny.deserialize(buf);
        }
        else if (kind == Kind::Medium)
        {
            tinyToMedium();
            medium.deserialize(buf);
        }
        else if (kind == Kind::Large)
        {
            tinyToLarge();
            large->deserialize(buf);
        }
    }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    UInt16 get(double level)
    {
        Kind kind = which();

        if (kind == Kind::Tiny)
        {
            tiny.prepare();
            return tiny.get(level);
        }
        else if (kind == Kind::Medium)
        {
            return medium.get(level);
        }
        else
        {
            return large->get(level);
        }
    }

    /// Get the size values of the quantiles of the `levels` levels. Record `size` results starting with `result` address.
    template <typename ResultType>
    void getMany(const double * levels, const size_t * levels_permutation, size_t size, ResultType * result)
    {
        Kind kind = which();

        if (kind == Kind::Tiny)
        {
            tiny.prepare();
            tiny.getMany(levels, size, result);
        }
        else if (kind == Kind::Medium)
        {
            medium.getMany(levels, levels_permutation, size, result);
        }
        else /*if (kind == Kind::Large)*/
        {
            large->getMany(levels, levels_permutation, size, result);
        }
    }

    /// The same, but in the case of an empty state, NaN is returned.
    float getFloat(double level)
    {
        return tiny.count
            ? get(level)
            : std::numeric_limits<float>::quiet_NaN();
    }

    void getManyFloat(const double * levels, const size_t * levels_permutation, size_t size, float * result)
    {
        if (tiny.count)
            getMany(levels, levels_permutation, size, result);
        else
            for (size_t i = 0; i < size; ++i)
                result[i] = std::numeric_limits<float>::quiet_NaN();
    }
};

#undef SMALL_THRESHOLD
#undef BIG_THRESHOLD
#undef BIG_SIZE
#undef BIG_PRECISION
#undef TINY_MAX_ELEMS

}
