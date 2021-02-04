#pragma once

#include <experimental/type_traits>
#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

template <typename T>
struct AggregationFunctionDeltaSumData
{
    T sum = 0;
    bool seen_last = false;
    T last;
    bool seen_first = false;
    T first;
};

template <typename T>
class AggregationFunctionDeltaSum final : public IAggregateFunctionDataHelper<
    AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>
{
public:
    AggregationFunctionDeltaSum(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<
          AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>> {arguments, params}
    {
        // empty constructor
    }

    String getName() const override 
    {
        return "deltaSum";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<T>>();
    }

    void ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const T & value = (*columns[0])[row_num].get<T>();

        if (this->data(place).last < value && this->data(place).seen_last) {
            this->data(place).sum += (value - this->data(place).last);
        }

        this->data(place).last = value;
        this->data(place).seen_last = true;

        if (this->data(place).seen_first == false) {
            this->data(place).first = value;
            this->data(place).seen_first = true;
        }
    }

    void ALWAYS_INLINE merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        if ((this->data(place).last < this->data(rhs).first) && this->data(place).seen_last && this->data(place).seen_first) {
            this->data(place).sum += this->data(rhs).sum + (this->data(rhs).first - this->data(place).last);
        } else {
            this->data(place).sum += this->data(rhs).sum;
        }

        this->data(place).last = this->data(rhs).last;
        this->data(place).first = this->data(rhs).first;
    }

        void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeIntBinary(this->data(place).sum, buf);
        writeIntBinary(this->data(place).first, buf);
        writeIntBinary(this->data(place).last, buf);
        writePODBinary<bool>(this->data(place).seen_first, buf);
        writePODBinary<bool>(this->data(place).seen_last, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readIntBinary(this->data(place).sum, buf);
        readIntBinary(this->data(place).first, buf);
        readIntBinary(this->data(place).last, buf);
        readPODBinary<bool>(this->data(place).seen_first, buf);
        readPODBinary<bool>(this->data(place).seen_last, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).sum);
    }
};

}
