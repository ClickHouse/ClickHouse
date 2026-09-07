#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class StatisticsUniqV2 : public IStatistics
{
public:
    StatisticsUniqV2(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
    ~StatisticsUniqV2() override;

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    UInt64 estimateCardinality() const override;
    bool isCompatibleWith(const IStatistics & other) const override;

    String getNameForLogs() const override { return "UniqV2 : " + std::to_string(estimateCardinality()); }

private:
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;
};

bool uniqV2StatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr uniqV2StatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
