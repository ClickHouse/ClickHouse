#pragma once

#include <Columns/ColumnVector.h>
#include <boost/noncopyable.hpp>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

/// Counts bitmap operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitmap final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitmap<T, Data>>
{
public:
    AggregateFunctionBitmap(const DataTypePtr & type)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionBitmap<T, Data>>({type}, {}) {}

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<T>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).rbs.add(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).rbs.merge(this->data(rhs).rbs);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).rbs.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).rbs.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).rbs.size());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


}
