#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <cmath>
#include <cfloat>


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
struct Settings;

class ReadBuffer;
class WriteBuffer;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


/// Returns tuple of (t-statistic, p-value) for both two-sample and one-sample t-tests
/// https://cpb-us-w2.wpmucdn.com/voices.uchicago.edu/dist/9/1193/files/2016/01/05b-TandP.pdf
template <typename Data, bool IsOneSample = false>
class AggregateFunctionTTest final:
    public IAggregateFunctionDataHelper<Data, AggregateFunctionTTest<Data, IsOneSample>>
{
private:
    bool need_confidence_interval = false;
    Float64 confidence_level;
    Float64 population_mean = 0.0;  // Only used for one-sample tests

public:
    AggregateFunctionTTest(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionTTest<Data, IsOneSample>>({arguments}, params, createResultType(!params.empty()))
    {
        if constexpr (IsOneSample)
        {
            // One-sample: requires population_mean parameter
            if (params.size() < 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} requires at least one parameter (population_mean).", Data::name);

            population_mean = params.at(0).safeGet<Float64>();
            if (!std::isfinite(population_mean))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} requires finite population mean.", Data::name);
        }

        // Handle confidence level parameter
        size_t confidence_param_index = IsOneSample ? 1 : 0;
        if (params.size() > confidence_param_index)
        {
            need_confidence_interval = true;
            confidence_level = params.at(confidence_param_index).safeGet<Float64>();

            if (!std::isfinite(confidence_level))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} requires finite confidence level.", Data::name);

            if (confidence_level <= 0.0 || confidence_level >= 1.0 || fabs(confidence_level - 0.0) < DBL_EPSILON || fabs(confidence_level - 1.0) < DBL_EPSILON)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Confidence level parameter must be between 0 and 1 in aggregate function {}.", Data::name);
        }
    }

    String getName() const override
    {
        return Data::name;
    }

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

            return std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            );
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
        if constexpr (IsOneSample)
        {
            // One-sample: single value input with population mean setup
            Float64 value = columns[0]->getFloat64(row_num);
            auto & data = this->data(place);
            
            if (data.n == 0)  // First value, set population mean
                data.setPopulationMean(population_mean);
                
            data.add(value);
        }
        else
        {
            // Two-sample: binary input with group indicator
            Float64 value = columns[0]->getFloat64(row_num);
            UInt8 is_second = columns[1]->getUInt(row_num);

            if (is_second)
                this->data(place).addY(value);
            else
                this->data(place).addX(value);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
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

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(t_statistic);
        column_value.getData().push_back(p_value);

        if (need_confidence_interval)
        {
            if constexpr (IsOneSample)
            {
                // One-sample: confidence intervals don't need degrees of freedom parameter
                auto [ci_low, ci_high] = data.getConfidenceIntervals(confidence_level);
                auto & column_ci_low = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(2));
                auto & column_ci_high = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(3));
                column_ci_low.getData().push_back(ci_low);
                column_ci_high.getData().push_back(ci_high);
            }
            else
            {
                // Two-sample: confidence intervals need degrees of freedom parameter
                auto [ci_low, ci_high] = data.getConfidenceIntervals(confidence_level, data.getDegreesOfFreedom());
                auto & column_ci_low = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(2));
                auto & column_ci_high = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(3));
                column_ci_low.getData().push_back(ci_low);
                column_ci_high.getData().push_back(ci_high);
            }
        }
    }
};

}
