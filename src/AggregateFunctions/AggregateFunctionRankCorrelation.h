#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatSample.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <Common/PODArray_fwd.h>
#include <common/types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

#include <DataTypes/DataTypeArray.h>

#include <Common/ArenaAllocator.h>

#include <type_traits>

#include <iostream>
namespace DB
{

template <typename X = Float64, typename Y = Float64>
struct AggregateFunctionRankCorrelationData final
{
    size_t size_x = 0;

    using AllocatorFirstSample = MixedAlignedArenaAllocator<alignof(X), 4096>;
    using FirstSample = PODArray<X, 32, AllocatorFirstSample>;

    using AllocatorSecondSample = MixedAlignedArenaAllocator<alignof(Y), 4096>;
    using SecondSample = PODArray<Y, 32, AllocatorSecondSample>;

    FirstSample first;
    SecondSample second;
};

template <typename X, typename Y>
class AggregateFunctionRankCorrelation :
    public IAggregateFunctionDataHelper<AggregateFunctionRankCorrelationData<X, Y>, AggregateFunctionRankCorrelation<X, Y>>
{
    using Data = AggregateFunctionRankCorrelationData<X, Y>;
    using FirstSample = typename Data::FirstSample;
    using SecondSample = typename Data::SecondSample;

public:
    explicit AggregateFunctionRankCorrelation(const DataTypes & arguments)
        :IAggregateFunctionDataHelper<AggregateFunctionRankCorrelationData<X, Y>,AggregateFunctionRankCorrelation<X, Y>> ({arguments}, {})
    {}

    String getName() const override
    {
        return "rankCorr";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void insert(Data & a, const std::pair<X, Y> & x, Arena * arena) const
    {
        ++a.size_x;
        a.first.push_back(x.first, arena);
        a.second.push_back(x.second, arena);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & a = this->data(place);

        auto new_x = assert_cast<const ColumnVector<X> &>(*columns[0]).getData()[row_num];
        auto new_y = assert_cast<const ColumnVector<Y> &>(*columns[1]).getData()[row_num];

        a.size_x += 1;

        a.first.push_back(new_x, arena);
        a.second.push_back(new_y, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = this->data(place);
        auto & b = this->data(rhs);

        if (b.size_x)
            for (size_t i = 0; i < b.size_x; ++i)
                insert(a, std::make_pair(b.first[i], b.second[i]), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & first = this->data(place).first;
        const auto & second = this->data(place).second;
        size_t size = this->data(place).size_x;
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(first.data()), size * sizeof(first[0]));
        buf.write(reinterpret_cast<const char *>(second.data()), size * sizeof(second[0]));
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        auto & first = this->data(place).first;

        first.resize(size, arena);
        buf.read(reinterpret_cast<char *>(first.data()), size * sizeof(first[0]));

        auto & second = this->data(place).second;

        second.resize(size, arena);
        buf.read(reinterpret_cast<char *>(second.data()), size * sizeof(second[0]));
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        /// Because ranks are adjusted, we have to store each of them in Float type.
        using RanksArray = PODArrayWithStackMemory<Float64, 32>;

        const auto & first = this->data(place).first;
        const auto & second = this->data(place).second;
        size_t size = this->data(place).size_x;

        RanksArray first_ranks;
        first_ranks.resize(first.size());
        computeRanks<FirstSample, RanksArray>(first, first_ranks);

        RanksArray second_ranks;
        second_ranks.resize(second.size());
        computeRanks<SecondSample, RanksArray>(second, second_ranks);

        // count d^2 sum
        Float64 answer = static_cast<Float64>(0);
        for (size_t j = 0; j < size; ++ j)
            answer += (first_ranks[j] - second_ranks[j]) * (first_ranks[j] - second_ranks[j]);

        answer *= 6;
        answer /= size * (size * size - 1);
        answer = 1 - answer;

        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(answer);
    }

};

};
