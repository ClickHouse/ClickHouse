#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class UniqStatistics : public IStatistics
{
public:
    UniqStatistics(const SingleStatisticsDescription & stat_, const DataTypePtr & data_type);

    ~UniqStatistics() override;

    UInt64 getCardinality();

    void serialize(WriteBuffer & buf) override;

    void deserialize(ReadBuffer & buf) override;

    void update(const ColumnPtr & column) override;

private:

    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;

};

StatisticsPtr UniqCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type);
void UniqValidator(const SingleStatisticsDescription &, DataTypePtr data_type);

}
