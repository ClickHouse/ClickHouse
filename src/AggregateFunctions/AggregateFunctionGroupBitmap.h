#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>

namespace DB
{
/// Counts bitmap operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitmap final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitmap<T, Data>>
{
public:
    AggregateFunctionBitmap(const DataTypePtr & type) : IAggregateFunctionDataHelper<Data, AggregateFunctionBitmap<T, Data>>({type}, {}) { }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).rbs.add(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).rbs.merge(this->data(rhs).rbs);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override { this->data(place).rbs.write(buf); }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override { this->data(place).rbs.read(buf); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).rbs.size());
    }
};


template <typename T, typename Data, typename Policy>
class AggregateFunctionBitmapL2 final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitmapL2<T, Data, Policy>>
{
public:
    AggregateFunctionBitmapL2(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionBitmapL2<T, Data, Policy>>({type}, {})
    {
    }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(assert_cast<const ColumnAggregateFunction &>(*columns[0]).getData()[row_num]);
        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.rbs.merge(data_rhs.rbs);
        }
        else
        {
            Policy::apply(data_lhs, data_rhs);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.rbs.merge(data_rhs.rbs);
        }
        else
        {
            Policy::apply(data_lhs, data_rhs);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override { this->data(place).rbs.write(buf); }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override { this->data(place).rbs.read(buf); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).rbs.size());
    }
};

template <typename Data>
class BitmapAndPolicy
{
public:
    static void apply(Data & lhs, const Data & rhs) { lhs.rbs.rb_and(rhs.rbs); }
};

template <typename Data>
class BitmapOrPolicy
{
public:
    static void apply(Data & lhs, const Data & rhs) { lhs.rbs.rb_or(rhs.rbs); }
};

template <typename Data>
class BitmapXorPolicy
{
public:
    static void apply(Data & lhs, const Data & rhs) { lhs.rbs.rb_xor(rhs.rbs); }
};

template <typename T, typename Data>
using AggregateFunctionBitmapL2And = AggregateFunctionBitmapL2<T, Data, BitmapAndPolicy<Data>>;

template <typename T, typename Data>
using AggregateFunctionBitmapL2Or = AggregateFunctionBitmapL2<T, Data, BitmapOrPolicy<Data>>;

template <typename T, typename Data>
using AggregateFunctionBitmapL2Xor = AggregateFunctionBitmapL2<T, Data, BitmapXorPolicy<Data>>;

}
