#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

#include <AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


template <typename T>
struct AggregateFunctionAvgData
{
    T sum = 0;
    UInt64 count = 0;
};


/// Calculates arithmetic mean of numbers.
template <typename T>
class AggregateFunctionAvg final : public IUnaryAggregateFunction<AggregateFunctionAvgData<typename NearestFieldType<T>::Type>, AggregateFunctionAvg<T>>
{
public:
    String getName() const override { return "avg"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void setArgument(const DataTypePtr & argument)
    {
        if (!argument->isNumeric())
            throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        this->data(place).sum += static_cast<const ColumnVector<T> &>(column).getData()[row_num];
        ++this->data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).sum, buf);
        writeVarUInt(this->data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).sum, buf);
        readVarUInt(this->data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnFloat64 &>(to).getData().push_back(
            static_cast<Float64>(this->data(place).sum) / this->data(place).count);
    }
};


}
