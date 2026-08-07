#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <IO/VarInt.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using AggregateFunctionAnalysisOfVarianceData = AnalysisOfVarianceMoments<Float64>;


/// One way analysis of variance
/// Provides a statistical test of whether two or more population means are equal (null hypothesis)
/// Has an assumption that subjects from group i have normal distribution.
/// Accepts two arguments - a value and a group number which this value belongs to.
/// Groups are enumerated starting from 0 and there should be at least two groups to perform a test
/// Moreover there should be at least one group with the number of observations greater than one.
class AggregateFunctionAnalysisOfVariance final : public IAggregateFunctionDataHelper<AggregateFunctionAnalysisOfVarianceData, AggregateFunctionAnalysisOfVariance>
{
public:
    explicit AggregateFunctionAnalysisOfVariance(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper(arguments, params, createResultType())
    {}

    DataTypePtr createResultType() const
    {
        DataTypes types {std::make_shared<DataTypeNumber<Float64>>(), std::make_shared<DataTypeNumber<Float64>>() };
        Strings names {"f_statistic", "p_value"};
        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    String getName() const override { return "analysisOfVariance"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).add(columns[0]->getFloat64(row_num), columns[1]->getUInt(row_num));
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto f_stat = data(place).getFStatistic();

        auto & column_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        if (unlikely(!std::isfinite(f_stat) || f_stat < 0))
        {
            column_stat.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
            column_value.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
            return;
        }

        auto p_value = data(place).getPValue(f_stat);

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        column_stat.getData().push_back(f_stat);
        column_value.getData().push_back(p_value);
    }

};

AggregateFunctionPtr createAggregateFunctionAnalysisOfVariance(const std::string & name, const DataTypes & arguments, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, arguments);

    if (!isNumber(arguments[0]))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} only supports numerical argument types", name);
    if (!WhichDataType(arguments[1]).isNativeUInt())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of aggregate function {} should be a native unsigned integer", name);

    return std::make_shared<AggregateFunctionAnalysisOfVariance>(arguments, parameters);
}

}

void registerAggregateFunctionAnalysisOfVariance(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .is_order_dependent = false };
    factory.registerFunction("analysisOfVariance", {createAggregateFunctionAnalysisOfVariance, properties}, AggregateFunctionFactory::Case::Insensitive);

    /// This is widely used term
    factory.registerAlias("anova", "analysisOfVariance", AggregateFunctionFactory::Case::Insensitive);
}

}
