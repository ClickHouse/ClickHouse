#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


static inline constexpr UInt64 GROUP_SORTED_DEFAULT_THRESHOLD = 0xFFFFFF;

namespace DB
{
template <typename T>
static void writeOneItem(WriteBuffer & buf, T item)
{
    if constexpr (std::numeric_limits<T>::is_signed)
    {
        writeVarInt(item, buf);
    }
    else
    {
        writeVarUInt(item, buf);
    }
}

static void writeOneItem(WriteBuffer & buf, const StringRef & item)
{
    writeBinary(item, buf);
}

template <typename T>
static void readOneItem(ReadBuffer & buf, Arena * /*arena*/, T & item)
{
    if constexpr (std::numeric_limits<T>::is_signed)
    {
        DB::Int64 val;
        readVarT(val, buf);
        item = val;
    }
    else
    {
        DB::UInt64 val;
        readVarT(val, buf);
        item = val;
    }
}

static void readOneItem(ReadBuffer & buf, Arena * arena, StringRef & item)
{
    item = readStringBinaryInto(*arena, buf);
}

template <typename Storage>
struct AggregateFunctionGroupArraySortedDataBase
{
    typedef typename Storage::value_type ValueType;
    AggregateFunctionGroupArraySortedDataBase(UInt64 threshold_ = GROUP_SORTED_DEFAULT_THRESHOLD) : threshold(threshold_) { }

    virtual ~AggregateFunctionGroupArraySortedDataBase() { }
    inline void narrowDown()
    {
        while (values.size() > threshold)
            values.erase(--values.end());
    }

    void merge(const AggregateFunctionGroupArraySortedDataBase & other)
    {
        values.merge(Storage(other.values));
        narrowDown();
    }

    void serialize(WriteBuffer & buf) const
    {
        writeOneItem(buf, UInt64(values.size()));
        for (auto value : values)
        {
            serializeItem(buf, value);
        }
    }

    virtual void serializeItem(WriteBuffer & buf, ValueType & val) const = 0;
    virtual ValueType deserializeItem(ReadBuffer & buf, Arena * arena) const = 0;

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        values.clear();
        UInt64 length;
        readOneItem(buf, nullptr, length);

        while (length--)
        {
            values.insert(deserializeItem(buf, arena));
        }

        narrowDown();
    }

    UInt64 threshold;
    Storage values;
};

template <typename T, bool expr_sorted, typename TIndex>
struct AggregateFunctionGroupArraySortedData
{
};

template <typename T, typename TIndex>
struct AggregateFunctionGroupArraySortedData<T, true, TIndex> : public AggregateFunctionGroupArraySortedDataBase<std::multimap<TIndex, T>>
{
    using Base = AggregateFunctionGroupArraySortedDataBase<std::multimap<TIndex, T>>;
    using Base::Base;

    void add(T item, TIndex weight)
    {
        Base::values.insert({weight, item});
        Base::narrowDown();
    }

    void serializeItem(WriteBuffer & buf, typename Base::ValueType & value) const override
    {
        writeOneItem(buf, value.first);
        writeOneItem(buf, value.second);
    }

    virtual typename Base::ValueType deserializeItem(ReadBuffer & buf, Arena * arena) const override
    {
        TIndex first;
        T second;
        readOneItem(buf, arena, first);
        readOneItem(buf, arena, second);

        return {first, second};
    }

    static T itemValue(typename Base::ValueType & value) { return value.second; }
};

template <typename T, typename TIndex>
struct AggregateFunctionGroupArraySortedData<T, false, TIndex> : public AggregateFunctionGroupArraySortedDataBase<std::multiset<T>>
{
    using Base = AggregateFunctionGroupArraySortedDataBase<std::multiset<T>>;
    using Base::Base;

    void add(T item)
    {
        Base::values.insert(item);
        Base::narrowDown();
    }

    void serializeItem(WriteBuffer & buf, typename Base::ValueType & value) const override { writeOneItem(buf, value); }

    typename Base::ValueType deserializeItem(ReadBuffer & buf, Arena * arena) const override
    {
        T value;
        readOneItem(buf, arena, value);
        return value;
    }

    static T itemValue(typename Base::ValueType & value) { return value; }
};
}
