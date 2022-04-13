#pragma once

#include <base/unaligned.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>

#include <Columns/ColumnsNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif

namespace DB
{
struct Settings;


/** Counts the number of unique values up to no more than specified in the parameter.
  *
  * Example: uniqUpTo(3)(UserID)
  * - will count the number of unique visitors, return 1, 2, 3 or 4 if visitors > = 4.
  *
  * For strings, a non-cryptographic hash function is used, due to which the calculation may be a bit inaccurate.
  */

template <typename T>
struct AggregateFunctionUniqUpToData
{
/** If count == threshold + 1 - this means that it is "overflowed" (values greater than threshold).
  * In this case (for example, after calling the merge function), the `data` array does not necessarily contain the initialized values
  * - example: combine a state in which there are few values, with another state that has overflowed;
  *   then set count to `threshold + 1`, and values from another state are not copied.
  */
    UInt8 count = 0;
    char data_ptr[0];

    T load(size_t i) const
    {
        return unalignedLoad<T>(data_ptr + i * sizeof(T));
    }

    void store(size_t i, const T & x)
    {
        unalignedStore<T>(data_ptr + i * sizeof(T), x);
    }

    size_t size() const
    {
        return count;
    }

    /// threshold - for how many elements there is room in a `data`.
    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE insert(T x, UInt8 threshold)
    {
        /// The state is already full - nothing needs to be done.
        if (count > threshold)
            return;

        /// Linear search for the matching element.
        for (size_t i = 0; i < count; ++i)
            if (load(i) == x)
                return;

        /// Did not find the matching element. If there is room for one more element, insert it.
        if (count < threshold)
            store(count, x);

        /// After increasing count, the state may be overflowed.
        ++count;
    }

    void merge(const AggregateFunctionUniqUpToData<T> & rhs, UInt8 threshold)
    {
        if (count > threshold)
            return;

        if (rhs.count > threshold)
        {
        /// If `rhs` is overflowed, then set `count` too also overflowed for the current state.
            count = rhs.count;
            return;
        }

        for (size_t i = 0; i < rhs.count; ++i)
            insert(rhs.load(i), threshold);
    }

    void write(WriteBuffer & wb, UInt8 threshold) const
    {
        writeBinary(count, wb);

        /// Write values only if the state is not overflowed. Otherwise, they are not needed, and only the fact that the state is overflowed is important.
        if (count <= threshold)
            wb.write(data_ptr, count * sizeof(T));
    }

    void read(ReadBuffer & rb, UInt8 threshold)
    {
        readBinary(count, rb);

        if (count <= threshold)
            rb.read(data_ptr, count * sizeof(T));
    }

    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE add(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        insert(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], threshold);
    }
};


/// For strings, their hashes are remembered.
template <>
struct AggregateFunctionUniqUpToData<String> : AggregateFunctionUniqUpToData<UInt64>
{
    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE add(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        /// Keep in mind that calculations are approximate.
        StringRef value = column.getDataAt(row_num);
        insert(CityHash_v1_0_2::CityHash64(value.data, value.size), threshold);
    }
};

template <>
struct AggregateFunctionUniqUpToData<UInt128> : AggregateFunctionUniqUpToData<UInt64>
{
    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE add(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        UInt128 value = assert_cast<const ColumnVector<UInt128> &>(column).getData()[row_num];
        insert(sipHash64(value), threshold);
    }
};

template <>
struct AggregateFunctionUniqUpToData<UInt256> : AggregateFunctionUniqUpToData<UInt64>
{
    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE add(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        UInt256 value = assert_cast<const ColumnVector<UInt256> &>(column).getData()[row_num];
        insert(sipHash64(value), threshold);
    }
};

template <>
struct AggregateFunctionUniqUpToData<Int256> : AggregateFunctionUniqUpToData<UInt64>
{
    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE add(const IColumn & column, size_t row_num, UInt8 threshold)
    {
        Int256 value = assert_cast<const ColumnVector<Int256> &>(column).getData()[row_num];
        insert(sipHash64(value), threshold);
    }
};


template <typename T>
class AggregateFunctionUniqUpTo final : public IAggregateFunctionDataHelper<AggregateFunctionUniqUpToData<T>, AggregateFunctionUniqUpTo<T>>
{
private:
    UInt8 threshold;

public:
    AggregateFunctionUniqUpTo(UInt8 threshold_, const DataTypes & argument_types_, const Array & params_)
        : IAggregateFunctionDataHelper<AggregateFunctionUniqUpToData<T>, AggregateFunctionUniqUpTo<T>>(argument_types_, params_)
        , threshold(threshold_)
    {
    }

    size_t sizeOfData() const override
    {
        return sizeof(AggregateFunctionUniqUpToData<T>) + sizeof(T) * threshold;
    }

    String getName() const override { return "uniqUpTo"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    /// ALWAYS_INLINE is required to have better code layout for uniqUpTo function
    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(*columns[0], row_num, threshold);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), threshold);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, threshold);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf, threshold);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
    }
};


/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of effective implementation), you can not pass several arguments, among which there are tuples.
  */
template <bool is_exact, bool argument_is_tuple>
class AggregateFunctionUniqUpToVariadic final
    : public IAggregateFunctionDataHelper<AggregateFunctionUniqUpToData<UInt64>, AggregateFunctionUniqUpToVariadic<is_exact, argument_is_tuple>>
{
private:
    size_t num_args = 0;
    UInt8 threshold;

public:
    AggregateFunctionUniqUpToVariadic(const DataTypes & arguments, const Array & params, UInt8 threshold_)
        : IAggregateFunctionDataHelper<AggregateFunctionUniqUpToData<UInt64>, AggregateFunctionUniqUpToVariadic<is_exact, argument_is_tuple>>(arguments, params)
        , threshold(threshold_)
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    size_t sizeOfData() const override
    {
        return sizeof(AggregateFunctionUniqUpToData<UInt64>) + sizeof(UInt64) * threshold;
    }

    String getName() const override { return "uniqUpTo"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insert(UInt64(UniqVariadicHash<is_exact, argument_is_tuple>::apply(num_args, columns, row_num)), threshold);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), threshold);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, threshold);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        this->data(place).read(buf, threshold);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size());
    }
};


}

#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
