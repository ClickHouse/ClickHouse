#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class StatisticsUniq : public IStatistics
{
public:
    StatisticsUniq(const SingleStatisticsDescription & statistics_description, const DataTypePtr & data_type);
    ~StatisticsUniq() override;

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    UInt64 estimateCardinality() const override;

private:
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;
};

void uniqStatisticsValidator(const SingleStatisticsDescription & statistics_description, DataTypePtr data_type);
StatisticsPtr uniqStatisticsCreator(const SingleStatisticsDescription & statistics_description, DataTypePtr data_type);

}
