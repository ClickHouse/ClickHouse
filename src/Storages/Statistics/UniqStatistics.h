#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class UniqStatistics : public IStatistics
{
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr uniq_collector;
    AggregateDataPtr data;

public:
    UniqStatistics(const SingleStatisticsDescription & stat_, const DataTypePtr & data_type);

    ~UniqStatistics() override;

    UInt64 getCardinality();

    void serialize(WriteBuffer & buf) override;

    void deserialize(ReadBuffer & buf) override;

    void update(const ColumnPtr & column) override;
};

StatisticsPtr UniqCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type);
void UniqValidator(const SingleStatisticsDescription &, DataTypePtr data_type);

}
