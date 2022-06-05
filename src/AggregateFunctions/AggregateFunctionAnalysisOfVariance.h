#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>

namespace DB
{
struct Settings;

class AggregateFunctionAnalysisOfVarianceData final : public AnalysisOfVarianceMoments<Float64> {
};

class AggregateFunctionAnalysisOfVariance final : public IAggregateFunctionDataHelper<AggregateFunctionAnalysisOfVarianceData, AggregateFunctionAnalysisOfVariance>
{
private:
    size_t n_groups_;

public:
    explicit AggregateFunctionAnalysisOfVariance(const DataTypes & arguments, const Array & params) : IAggregateFunctionDataHelper(arguments, params) {
        size_t n_groups = params.at(0).safeGet<UInt64>();
        n_groups_ = n_groups;
    }

    DataTypePtr getReturnType() const override
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>()
        };

        Strings names
        {
            "f_statistic",
            "p_value"
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    String getName() const override { return "anova"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).add(columns[0]->getFloat64(row_num), columns[1]->getUInt(row_num), n_groups_);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs), n_groups_);
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
        if (!std::isfinite(f_stat)) {
            throw Exception("F statistic is not defined or infinite for these arguments", ErrorCodes::BAD_ARGUMENTS);
        }
        auto p_value = data(place).getPValue();

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(f_stat);
        column_value.getData().push_back(p_value);
    }

};

}
