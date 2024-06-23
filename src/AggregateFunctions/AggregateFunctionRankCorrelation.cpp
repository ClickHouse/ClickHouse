#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/PODArray_fwd.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB
{
struct Settings;

namespace
{

struct RankCorrelationData : public StatisticalSample<Float64, Float64>
{
    Float64 getResult()
    {
        RanksArray ranks_x;
        std::tie(ranks_x, std::ignore) = computeRanksAndTieCorrection(this->x);

        RanksArray ranks_y;
        std::tie(ranks_y, std::ignore) = computeRanksAndTieCorrection(this->y);

        /// Sizes can be non-equal due to skipped NaNs.
        const Float64 size = static_cast<Float64>(std::min(this->size_x, this->size_y));

        /// Count d^2 sum
        Float64 answer = 0;
        for (size_t j = 0; j < size; ++j)
            answer += (ranks_x[j] - ranks_y[j]) * (ranks_x[j] - ranks_y[j]);

        answer *= 6;
        answer /= size * (size * size - 1);
        answer = 1 - answer;
        return answer;
    }
};

class AggregateFunctionRankCorrelation :
    public IAggregateFunctionDataHelper<RankCorrelationData, AggregateFunctionRankCorrelation>
{
public:
    explicit AggregateFunctionRankCorrelation(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<RankCorrelationData, AggregateFunctionRankCorrelation> ({arguments}, {}, std::make_shared<DataTypeNumber<Float64>>())
    {}

    String getName() const override
    {
        return "rankCorr";
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 new_x = columns[0]->getFloat64(row_num);
        Float64 new_y = columns[1]->getFloat64(row_num);
        data(place).addX(new_x, arena);
        data(place).addY(new_y, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = data(place);
        const auto & b = data(rhs);

        a.merge(b, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto answer = data(place).getResult();

        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(answer);
    }

};


AggregateFunctionPtr createAggregateFunctionRankCorrelation(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionRankCorrelation>(argument_types);
}

}


void registerAggregateFunctionRankCorrelation(AggregateFunctionFactory & factory)
{
    factory.registerFunction("rankCorr", createAggregateFunctionRankCorrelation);
}

}
