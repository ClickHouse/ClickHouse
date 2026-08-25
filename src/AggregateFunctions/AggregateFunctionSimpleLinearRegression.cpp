#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct AggregateFunctionSimpleLinearRegressionData final
{
    size_t count = 0;
    Float64 sum_x = 0;
    Float64 sum_y = 0;
    Float64 sum_xx = 0;
    Float64 sum_xy = 0;

    void add(Float64 x, Float64 y)
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

    Float64 getK() const
    {
        Float64 divisor = sum_xx * count - sum_x * sum_x;

        if (divisor == 0)
            return std::numeric_limits<Float64>::quiet_NaN();

        return (sum_xy * count - sum_x * sum_y) / divisor;
    }

    Float64 getB(Float64 k) const
    {
        if (count == 0)
            return std::numeric_limits<Float64>::quiet_NaN();

        return (sum_y - k * sum_x) / count;
    }
};

/// Calculates simple linear regression parameters.
/// Result is a tuple (k, b) for y = k * x + b equation, solved by least squares approximation.
class AggregateFunctionSimpleLinearRegression final : public IAggregateFunctionDataHelper<
    AggregateFunctionSimpleLinearRegressionData,
    AggregateFunctionSimpleLinearRegression>
{
public:
    AggregateFunctionSimpleLinearRegression(
        const DataTypes & arguments,
        const Array & params
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionSimpleLinearRegressionData,
            AggregateFunctionSimpleLinearRegression
        > {arguments, params, createResultType()}
    {
        // notice: arguments has been checked before
    }

    String getName() const override
    {
        return "simpleLinearRegression";
    }

    void add(
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        size_t row_num,
        Arena *
    ) const override
    {
        Float64 x = columns[0]->getFloat64(row_num);
        Float64 y = columns[1]->getFloat64(row_num);

        data(place).add(x, y);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    static DataTypePtr createResultType()
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>(),
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
        AggregateDataPtr __restrict place,
        IColumn & to,
        Arena *) const override
    {
        Float64 k = data(place).getK();
        Float64 b = data(place).getB(k);

        auto & col_tuple = assert_cast<ColumnTuple &>(to);
        auto & col_k = assert_cast<ColumnVector<Float64> &>(col_tuple.getColumn(0));
        auto & col_b = assert_cast<ColumnVector<Float64> &>(col_tuple.getColumn(1));

        col_k.getData().push_back(k);
        col_b.getData().push_back(b);
    }
};


AggregateFunctionPtr createAggregateFunctionSimpleLinearRegression(
    const String & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    if (!isNumber(arguments[0]) || !isNumber(arguments[1]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types ({}, {}) of arguments of aggregate function {}, must "
            "be Native Ints, Native UInts or Floats", arguments[0]->getName(), arguments[1]->getName(), name);

    return std::make_shared<AggregateFunctionSimpleLinearRegression>(arguments, params);
}

}

void registerAggregateFunctionSimpleLinearRegression(AggregateFunctionFactory & factory)
{
    factory.registerFunction("simpleLinearRegression", createAggregateFunctionSimpleLinearRegression);
}

}
