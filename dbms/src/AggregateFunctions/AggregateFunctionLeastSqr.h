#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename X, typename Y, typename Ret>
struct AggregateFunctionLeastSqrData final
{
    size_t count = 0;
    Ret sum_x = 0;
    Ret sum_y = 0;
    Ret sum_xx = 0;
    Ret sum_xy = 0;

    void add(X x, Y y)
    {
        count += 1;
        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_xy += x * y;
    }

    void merge(const AggregateFunctionLeastSqrData & other)
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

    Ret getK() const
    {
        Ret divisor = sum_xx * count - sum_x * sum_x;

        if (divisor == 0)
            return std::numeric_limits<Ret>::quiet_NaN();

        return (sum_xy * count - sum_x * sum_y) / divisor;
    }

    Ret getB(Ret k) const
    {
        if (count == 0)
            return std::numeric_limits<Ret>::quiet_NaN();

        return (sum_y - k * sum_x) / count;
    }
};

/// Calculates simple linear regression parameters.
/// Result is a tuple (k, b) for y = k * x + b equation, solved by least squares approximation.
template <typename X, typename Y, typename Ret = Float64>
class AggregateFunctionLeastSqr final : public IAggregateFunctionDataHelper<
    AggregateFunctionLeastSqrData<X, Y, Ret>,
    AggregateFunctionLeastSqr<X, Y, Ret>
>
{
public:
    AggregateFunctionLeastSqr(
        const DataTypes & arguments,
        const Array & params
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionLeastSqrData<X, Y, Ret>,
            AggregateFunctionLeastSqr<X, Y, Ret>
        > {arguments, params}
    {
        // notice: arguments has been checked before
    }

    String getName() const override
    {
        return "leastSqr";
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena *
    ) const override
    {
        auto col_x {
            static_cast<const ColumnVector<X> *>(columns[0])
        };
        auto col_y {
            static_cast<const ColumnVector<Y> *>(columns[1])
        };

        X x = col_x->getData()[row_num];
        Y y = col_y->getData()[row_num];

        this->data(place).add(x, y);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs, Arena *
    ) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf
    ) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf, Arena *
    ) const override
    {
        this->data(place).deserialize(buf);
    }

    DataTypePtr getReturnType() const override
    {
        DataTypes types {
            std::make_shared<DataTypeNumber<Ret>>(),
            std::make_shared<DataTypeNumber<Ret>>(),
        };

        Strings names {
            "k",
            "b",
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    void insertResultInto(
        ConstAggregateDataPtr place,
        IColumn & to
    ) const override
    {
        Ret k = this->data(place).getK();
        Ret b = this->data(place).getB(k);

        auto & col_tuple = static_cast<ColumnTuple &>(to);
        auto & col_k = static_cast<ColumnVector<Ret> &>(col_tuple.getColumn(0));
        auto & col_b = static_cast<ColumnVector<Ret> &>(col_tuple.getColumn(1));

        col_k.getData().push_back(k);
        col_b.getData().push_back(b);
    }
};

}
