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
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

template <typename T>
struct MovingData
{
    using Accumulator = T;

    /// Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;    /// Prefix sums.
    T sum{};

    void NO_SANITIZE_UNDEFINED add(T val, Arena * arena)
    {
        sum += val;
        value.push_back(sum, arena);
    }
};

template <typename T>
struct MovingSumData : public MovingData<T>
{
    static constexpr auto name = "groupArrayMovingSum";

    T NO_SANITIZE_UNDEFINED get(size_t idx, UInt64 window_size) const
    {
        if (idx < window_size)
            return this->value[idx];
        else
            return this->value[idx] - this->value[idx - window_size];
    }
};

template <typename T>
struct MovingAvgData : public MovingData<T>
{
    static constexpr auto name = "groupArrayMovingAvg";

    T NO_SANITIZE_UNDEFINED get(size_t idx, UInt64 window_size) const
    {
        if (idx < window_size)
            return this->value[idx] / T(window_size);
        else
            return (this->value[idx] - this->value[idx - window_size]) / T(window_size);
    }
};


template <typename T, typename Tlimit_num_elems, typename Data>
class MovingImpl final
    : public IAggregateFunctionDataHelper<Data, MovingImpl<T, Tlimit_num_elems, Data>>
{
    static constexpr bool limit_num_elems = Tlimit_num_elems::value;
    UInt64 window_size;

public:
    using ResultT = typename Data::Accumulator;

    using ColumnSource = std::conditional_t<IsDecimalNumber<T>,
        ColumnDecimal<T>,
        ColumnVector<T>>;

    /// Probably for overflow function in the future.
    using ColumnResult = std::conditional_t<IsDecimalNumber<ResultT>,
        ColumnDecimal<ResultT>,
        ColumnVector<ResultT>>;

    using DataTypeResult = std::conditional_t<IsDecimalNumber<ResultT>,
        DataTypeDecimal<ResultT>,
        DataTypeNumber<ResultT>>;

    explicit MovingImpl(const DataTypePtr & data_type_, UInt64 window_size_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<Data, MovingImpl<T, Tlimit_num_elems, Data>>({data_type_}, {})
        , window_size(window_size_) {}

    String getName() const override { return Data::name; }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<ResultT>)
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeResult>(
                DataTypeResult::maxPrecision(), getDecimalScale(*this->argument_types.at(0))));
        else
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeResult>());
    }

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto value = static_cast<const ColumnSource &>(*columns[0]).getData()[row_num];
        this->data(place).add(static_cast<ResultT>(value), arena);
    }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
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

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (size > 0)
        {
            auto & value = this->data(place).value;
            value.resize(size, arena);
            buf.read(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));
            this->data(place).sum = value.back();
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        size_t size = data.value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnResult::Container & data_to = assert_cast<ColumnResult &>(arr_to.getData()).getData();

            for (size_t i = 0; i < size; ++i)
            {
                if (!limit_num_elems)
                {
                    data_to.push_back(data.get(i, size));
                }
                else
                {
                    data_to.push_back(data.get(i, window_size));
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
