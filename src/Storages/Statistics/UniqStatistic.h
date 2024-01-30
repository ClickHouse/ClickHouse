#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class UniqStatistic : public IStatistic
{
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr uniq_collector;
    AggregateDataPtr data;
    Int64 result;
public:
    explicit UniqStatistic(const StatisticDescription & stat_, DataTypePtr data_type) : IStatistic(stat_), result(-1)
    {
        arena = std::make_unique<Arena>();
        AggregateFunctionProperties property;
        property.returns_default_when_only_null = true;
        uniq_collector = AggregateFunctionFactory::instance().get("uniq", NullsAction::IGNORE_NULLS, {data_type}, Array(), property);
        data = arena->alignedAlloc(uniq_collector->sizeOfData(), uniq_collector->alignOfData());
        uniq_collector->create(data);
    }

    ~UniqStatistic() override
    {
        uniq_collector->destroy(data);
    }

    Int64 getCardinality()
    {
        if (result < 0)
        {
            auto column = DataTypeInt64().createColumn();
            uniq_collector->insertResultInto(data, *column, nullptr);
            result = column->getInt(0);
        }
        return result;
    }

    void serialize(WriteBuffer & buf) override
    {
        uniq_collector->serialize(data, buf);
    }

    void deserialize(ReadBuffer & buf) override
    {
        uniq_collector->deserialize(data, buf);
    }

    void update(const ColumnPtr & column) override
    {
        const IColumn * col_ptr = column.get();
        uniq_collector->add(data, &col_ptr, column->size(), nullptr);
    }
};

}
