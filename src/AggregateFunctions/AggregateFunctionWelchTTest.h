#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>
#include <cmath>
#include <functional>

#include <type_traits>

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{

template <typename X = Float64, typename Y = Float64>
struct AggregateFunctionWelchTTestData final
{
    size_t size_x = 0;
    size_t size_y = 0;
    X sum_x = static_cast<X>(0);
    Y sum_y = static_cast<Y>(0);
    X square_sum_x = static_cast<X>(0);
    Y square_sum_y = static_cast<Y>(0);
    Float64 mean_x = static_cast<Float64>(0);
    Float64 mean_y = static_cast<Float64>(0);

    void add(X x, Y y)
    {
        sum_x += x;
        sum_y += y;
        size_x++;
        size_y++;
        mean_x = static_cast<Float64>(sum_x) / size_x;
        mean_y = static_cast<Float64>(sum_y) / size_y;
        square_sum_x += x * x;
        square_sum_y += y * y;
    }

    void merge(const AggregateFunctionWelchTTestData &other)
    {
        sum_x += other.sum_x;
        sum_y += other.sum_y;
        size_x += other.size_x;
        size_y += other.size_y;
        mean_x = static_cast<Float64>(sum_x) / size_x;
        mean_y = static_cast<Float64>(sum_y) / size_y;
        square_sum_x += other.square_sum_x;
        square_sum_y += other.square_sum_y;
    }

    void serialize(WriteBuffer &buf) const
    {
        writeBinary(mean_x, buf);
        writeBinary(mean_y, buf);
        writeBinary(sum_x, buf);
        writeBinary(sum_y, buf);
        writeBinary(square_sum_x, buf);
        writeBinary(square_sum_y, buf);
        writeBinary(size_x, buf);
        writeBinary(size_y, buf);
    }

    void deserialize(ReadBuffer &buf)
    {
        readBinary(mean_x, buf);
        readBinary(mean_y, buf);
        readBinary(sum_x, buf);
        readBinary(sum_y, buf);
        readBinary(square_sum_x, buf);
        readBinary(square_sum_y, buf);
        readBinary(size_x, buf);
        readBinary(size_y, buf);
    }

    size_t getSizeY() const
    {
        return size_y;
    }

    size_t getSizeX() const
    {
        return size_x;
    }

    Float64 getSxSquared() const
    {
        /// The original formulae looks like  \frac{1}{size_x - 1} \sum_{i = 1}^{size_x}{(x_i - \bar{x}) ^ 2}
        /// But we made some mathematical transformations not to store original sequences.
        /// Also we dropped sqrt, because later it will be squared later.
        return static_cast<Float64>(square_sum_x + size_x * std::pow(mean_x, 2) - 2 * mean_x * sum_x) / (size_x - 1);
    }

    Float64 getSySquared() const
    {
        /// The original formulae looks like  \frac{1}{size_y - 1} \sum_{i = 1}^{size_y}{(y_i - \bar{y}) ^ 2}
        /// But we made some mathematical transformations not to store original sequences.
        /// Also we dropped sqrt, because later it will be squared later.
        return static_cast<Float64>(square_sum_y + size_y * std::pow(mean_y, 2) - 2 * mean_y * sum_y) / (size_y - 1);
    }

    Float64 getTStatisticSquared() const
    {
        if (size_x == 0 || size_y == 0)
        {
            throw Exception("Division by zero encountered in Aggregate function WelchTTest", ErrorCodes::BAD_ARGUMENTS);
        }

        return std::pow(mean_x - mean_y, 2) / (getSxSquared() / size_x + getSySquared() / size_y);
    }

    Float64 getDegreesOfFreedom() const
    {
        auto sx = getSxSquared();
        auto sy = getSySquared();
        Float64 numerator = std::pow(sx / size_x + sy / size_y, 2);
        Float64 denominator_first = std::pow(sx, 2) / (std::pow(size_x, 2) * (size_x - 1));
        Float64 denominator_second = std::pow(sy, 2) / (std::pow(size_y, 2) * (size_y - 1));
        return numerator / (denominator_first + denominator_second);
    }

    static Float64 integrateSimpson(Float64 a, Float64 b, std::function<Float64(Float64)> func, size_t iterations = 1e6)
    {
        double h = (b - a) / iterations;
        Float64 sum_odds = 0.0;
        for (size_t i = 1; i < iterations; i += 2)
            sum_odds += func(a + i * h);
        Float64 sum_evens = 0.0;
        for (size_t i = 2; i < iterations; i += 2)
            sum_evens += func(a + i * h);
        return (func(a) + func(b) + 2 * sum_evens + 4 * sum_odds) * h / 3;
    }

    Float64 getPValue() const
    {
        const Float64 v = getDegreesOfFreedom();
        const Float64 t = getTStatisticSquared();
        auto f = [&v] (double x) { return std::pow(x, v/2 - 1) / std::sqrt(1 - x); };
        Float64 numenator = integrateSimpson(0, v / (t + v), f);
        Float64 denominator = std::exp(std::lgammal(v/2) + std::lgammal(0.5) - std::lgammal(v/2 + 0.5));
        return numenator / denominator;
    }

    Float64 getResult() const
    {
        return getPValue();
    }
};

/// Returns p-value
template <typename X = Float64, typename Y = Float64>
class AggregateFunctionWelchTTest : 
    public IAggregateFunctionDataHelper<AggregateFunctionWelchTTestData<X, Y>,AggregateFunctionWelchTTest<X, Y>>
{

public:
    AggregateFunctionWelchTTest(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionWelchTTestData<X, Y>, AggregateFunctionWelchTTest<X, Y>> ({arguments}, {})
    {}

    String getName() const override
    {
        return "WelchTTest";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
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

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * /*arena*/) const override
    {
        size_t size_x = this->data(place).getSizeX();
        size_t size_y = this->data(place).getSizeY();

        if (size_x < 2 || size_y < 2)
        {
            throw Exception("Aggregate function " + getName() + " requires samples to be of size > 1", ErrorCodes::BAD_ARGUMENTS);
        }

        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(this->data(place).getResult());
    }

};

};
