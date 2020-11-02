#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <cmath>


/// This function is used in implementations of different T-Tests.
/// On Darwin it's unavailable in math.h but actually exists in the library (can be linked successfully).
#if defined(OS_DARWIN)
extern "C"
{
    double lgamma_r(double x, int * signgamp);
}
#endif


namespace DB
{

class ReadBuffer;
class WriteBuffer;


template <typename F>
static Float64 integrateSimpson(Float64 a, Float64 b, F && func)
{
    const size_t iterations = std::max(1e6, 1e4 * std::abs(std::round(b)));
    const long double h = (b - a) / iterations;
    Float64 sum_odds = 0.0;
    for (size_t i = 1; i < iterations; i += 2)
        sum_odds += func(a + i * h);
    Float64 sum_evens = 0.0;
    for (size_t i = 2; i < iterations; i += 2)
        sum_evens += func(a + i * h);
    return (func(a) + func(b) + 2 * sum_evens + 4 * sum_odds) * h / 3;
}

static inline Float64 getPValue(Float64 degrees_of_freedom, Float64 t_stat2)
{
    Float64 numerator = integrateSimpson(0, degrees_of_freedom / (t_stat2 + degrees_of_freedom),
        [degrees_of_freedom](double x) { return std::pow(x, degrees_of_freedom / 2 - 1) / std::sqrt(1 - x); });

    int unused;
    Float64 denominator = std::exp(
        lgamma_r(degrees_of_freedom / 2, &unused)
        + lgamma_r(0.5, &unused)
        - lgamma_r(degrees_of_freedom / 2 + 0.5, &unused));

    return std::min(1.0, std::max(0.0, numerator / denominator));
}


/// Returns tuple of (t-statistic, p-value)
/// https://cpb-us-w2.wpmucdn.com/voices.uchicago.edu/dist/9/1193/files/2016/01/05b-TandP.pdf
template <typename Data>
class AggregateFunctionTTest :
    public IAggregateFunctionDataHelper<Data, AggregateFunctionTTest<Data>>
{
public:
    AggregateFunctionTTest(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionTTest<Data>>({arguments}, {})
    {
    }

    String getName() const override
    {
        return Data::name;
    }

    DataTypePtr getReturnType() const override
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>(),
        };

        Strings names
        {
            "t_statistic",
            "p_value"
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Float64 x = columns[0]->getFloat64(row_num);
        Float64 y = columns[1]->getFloat64(row_num);

        this->data(place).add(x, y);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        auto [t_statistic, p_value] = this->data(place).getResult();

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(t_statistic);
        column_value.getData().push_back(p_value);
    }
};

};
