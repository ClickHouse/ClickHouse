#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class StatisticsUniq : public ISingleStatistics
{
public:
    StatisticsUniq(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
    ~StatisticsUniq() override;

    void update(const ColumnPtr & column) override;
    void merge(const SingleStatisticsPtr & other) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    UInt64 estimateCardinality() const override;

private:
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;

};

void uniqStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
SingleStatisticsPtr uniqStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
