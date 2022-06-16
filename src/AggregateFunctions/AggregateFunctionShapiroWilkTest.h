#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>

namespace DB
{
    struct Settings;

    class AggregateFunctionShapiroWilkTestData final : public ShapiroWilkTestSample<Float64> {
    };

    class AggregateFunctionShapiroWilkTest final : public IAggregateFunctionDataHelper<AggregateFunctionShapiroWilkTestData, AggregateFunctionShapiroWilkTest>
    {
    private:

    public:
        explicit AggregateFunctionShapiroWilkTest(const DataTypes & arguments, const Array & params) : IAggregateFunctionDataHelper(arguments, params) {
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
                            "w_statistic",
                            "p_value"
                    };

            return std::make_shared<DataTypeTuple>(
                    std::move(types),
                    std::move(names)
            );
        }

        String getName() const override { return "shapiro"; }

        bool allocatesMemoryInArena() const override { return true; }

        void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
        {
            data(place).add(columns[0]->getFloat64(row_num), arena);
        }

        void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
        {
            data(place).merge(data(rhs), arena);
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
            auto w_stat = data(place).getWStatistic();
            if (!std::isfinite(w_stat)) {
                throw Exception("W statistic is not defined or infinite for these arguments", ErrorCodes::BAD_ARGUMENTS);
            }
            auto p_value = data(place).getPValue(w_stat);

            /// Because p-value is a probability.
            p_value = std::min(1.0, std::max(0.0, p_value));

            auto & column_tuple = assert_cast<ColumnTuple &>(to);
            auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
            auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

            column_stat.getData().push_back(w_stat);
            column_value.getData().push_back(p_value);
        }

    };

}
