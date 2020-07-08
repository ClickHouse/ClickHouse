#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <type_traits>

#define AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}


template <typename T>
struct MovingSumData
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;
    Array window;
    T sum = 0;

    void add(T val, Arena * arena)
    {
        sum += val;

        value.push_back(sum, arena);
    }

    T get(size_t idx, UInt64 win_size) const
    {
        if (idx < win_size)
            return value[idx];
        else
            return value[idx] - value[idx - win_size];
    }

};

template <typename T>
struct MovingAvgData
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;
    Array window;
    T sum = 0;

    void add(T val, Arena * arena)
    {
        sum += val;
        value.push_back(sum, arena);
    }

    T get(size_t idx, UInt64 win_size) const
    {
        if (idx < win_size)
            return value[idx] / win_size;
        else
            return (value[idx] - value[idx - win_size]) / win_size;
    }

};


template <typename T, typename Tlimit_num_elems, typename Data>
class MovingImpl final
    : public IAggregateFunctionDataHelper<Data, MovingImpl<T, Tlimit_num_elems, Data>>
{
    static constexpr bool limit_num_elems = Tlimit_num_elems::value;
    DataTypePtr & data_type;
    UInt64 win_size;

public:
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    // probably for overflow function in the future
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

    explicit MovingImpl(const DataTypePtr & data_type_, UInt64 win_size_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<Data, MovingImpl<T, Tlimit_num_elems, Data>>({data_type_}, {})
        , data_type(this->argument_types[0]), win_size(win_size_) {}

    String getName() const override { return "movingXXX"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(data_type);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto val = static_cast<const ColVecType &>(*columns[0]).getData()[row_num];

        this->data(place).add(val, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        size_t cur_size = cur_elems.value.size();

        if (rhs_elems.value.size())
            cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.end(), arena);

        for (size_t i = cur_size; i < cur_elems.value.size(); ++i)
        {
            cur_elems.value[i] += cur_elems.sum;
        }

        cur_elems.sum += rhs_elems.sum;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        auto & value = this->data(place).value;

        value.resize(size, arena);
        buf.read(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));

        this->data(place).sum = value.back();
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        size_t size = data.value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColVecResult::Container & data_to = static_cast<ColVecResult &>(arr_to.getData()).getData();

            for (size_t i = 0; i < size; ++i)
            {
                if (!limit_num_elems)
                {
                    data_to.push_back(data.get(i, size));
                }
                else
                {
                    data_to.push_back(data.get(i, win_size));
                }
            }
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }
};

#undef AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE

}
