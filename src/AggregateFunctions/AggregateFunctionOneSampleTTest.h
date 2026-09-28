#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <cmath>
#include <cfloat>

namespace DB
{
struct Settings;
namespace ErrorCodes { extern const int BAD_ARGUMENTS; }

template <typename Data>
class AggregateFunctionOneSampleTTest final:
    public IAggregateFunctionDataHelper<Data, AggregateFunctionOneSampleTTest<Data>>
{
private:
    bool need_confidence_interval = false;
    Float64 confidence_level;

public:
    AggregateFunctionOneSampleTTest(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionOneSampleTTest<Data>>({arguments}, params, createResultType(!params.empty()))
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} requires two arguments: sample_data and population_mean.", Data::name);

        if (!params.empty())
        {
            need_confidence_interval = true;
            confidence_level = params.at(0).safeGet<Float64>();

            if (!std::isfinite(confidence_level))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} requires finite confidence level.", Data::name);

            if (confidence_level <= 0.0 || confidence_level >= 1.0 || fabs(confidence_level - 0.0) < DBL_EPSILON || fabs(confidence_level - 1.0) < DBL_EPSILON)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Confidence level parameter must be between 0 and 1 in aggregate function {}.", Data::name);
        }
    }

    String getName() const override { return Data::name; }

    static DataTypePtr createResultType(bool need_confidence_interval_)
    {
        if (need_confidence_interval_)
        {
            DataTypes types
            {
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
            };

            Strings names
            {
                "t_statistic",
                "p_value",
                "confidence_interval_low",
                "confidence_interval_high",
            };

            return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
        }

        DataTypes types{
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>(),
        };

        Strings names{
            "t_statistic",
            "p_value",
        };

        return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data = this->data(place);
        Float64 value = columns[0]->getFloat64(row_num);

        if (data.n == 0)
        {
            data.setPopulationMean(columns[1]->getFloat64(row_num));
        }

        data.add(value);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = this->data(place);
        auto & column_tuple = assert_cast<ColumnTuple &>(to);

        if (!data.hasEnoughObservations() || data.isEssentiallyConstant())
        {
            auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
            auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));
            column_stat.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
            column_value.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());

            if (need_confidence_interval)
            {
                auto & column_ci_low = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(2));
                auto & column_ci_high = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(3));
                column_ci_low.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
                column_ci_high.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
            }

            return;
        }

        auto [t_statistic, p_value] = data.getResult();
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));
        column_stat.getData().push_back(t_statistic);
        column_value.getData().push_back(p_value);

        if (need_confidence_interval)
        {
            auto [ci_low, ci_high] = data.getConfidenceIntervals(confidence_level);
            auto & column_ci_low = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(2));
            auto & column_ci_high = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(3));
            column_ci_low.getData().push_back(ci_low);
            column_ci_high.getData().push_back(ci_high);
        }
    }
};

}

