#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
}

template <typename T>
struct AggregateFunctionSimpleLinearRegressionData final
{
    size_t count = 0;
    T sum_x = 0;
    T sum_y = 0;
    T sum_xx = 0;
    T sum_xy = 0;

    void add(T x, T y)
    {
        count += 1;
        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_xy += x * y;
    }

    void merge(const AggregateFunctionSimpleLinearRegressionData & other)
    {
        count += other.count;
        sum_x += other.sum_x;
        sum_y += other.sum_y;
        sum_xx += other.sum_xx;
        sum_xy += other.sum_xy;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count, buf);
        writeBinary(sum_x, buf);
        writeBinary(sum_y, buf);
        writeBinary(sum_xx, buf);
        writeBinary(sum_xy, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(count, buf);
        readBinary(sum_x, buf);
        readBinary(sum_y, buf);
        readBinary(sum_xx, buf);
        readBinary(sum_xy, buf);
    }

    T getK() const
    {
        T divisor = sum_xx * count - sum_x * sum_x;

        if (divisor == 0)
            return std::numeric_limits<T>::quiet_NaN();

        return (sum_xy * count - sum_x * sum_y) / divisor;
    }

    T getB(T k) const
    {
        if (count == 0)
            return std::numeric_limits<T>::quiet_NaN();

        return (sum_y - k * sum_x) / count;
    }
};

/// Calculates simple linear regression parameters.
/// Result is a tuple (k, b) for y = k * x + b equation, solved by least squares approximation.
template <typename X, typename Y, typename Ret = Float64>
class AggregateFunctionSimpleLinearRegression final : public IAggregateFunctionDataHelper<
    AggregateFunctionSimpleLinearRegressionData<Ret>,
    AggregateFunctionSimpleLinearRegression<X, Y, Ret>
>
{
public:
    AggregateFunctionSimpleLinearRegression(
        const DataTypes & arguments,
        const Array & params
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionSimpleLinearRegressionData<Ret>,
            AggregateFunctionSimpleLinearRegression<X, Y, Ret>
        > {arguments, params}
    {
        // notice: arguments has been checked before
    }

    String getName() const override
    {
        return "simpleLinearRegression";
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena *
    ) const override
    {
        auto col_x = assert_cast<const ColumnVector<X> *>(columns[0]);
        auto col_y = assert_cast<const ColumnVector<Y> *>(columns[1]);

        X x = col_x->getData()[row_num];
        Y y = col_y->getData()[row_num];

        this->data(place).add(x, y);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    DataTypePtr getReturnType() const override
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Ret>>(),
            std::make_shared<DataTypeNumber<Ret>>(),
        };

        Strings names
        {
            "k",
            "b",
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    bool allocatesMemoryInArena() const override { return false; }

    void insertResultInto(
        AggregateDataPtr place,
        IColumn & to,
        Arena *) const override
    {
        Ret k = this->data(place).getK();
        Ret b = this->data(place).getB(k);

        auto & col_tuple = assert_cast<ColumnTuple &>(to);
        auto & col_k = assert_cast<ColumnVector<Ret> &>(col_tuple.getColumn(0));
        auto & col_b = assert_cast<ColumnVector<Ret> &>(col_tuple.getColumn(1));

        col_k.getData().push_back(k);
        col_b.getData().push_back(b);
    }
};

}
