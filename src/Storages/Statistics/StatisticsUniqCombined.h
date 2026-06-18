#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class StatisticsUniqCombined : public IStatistics
{
public:
    StatisticsUniqCombined(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
    ~StatisticsUniqCombined() override;

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    UInt64 estimateCardinality() const override;
    bool isCompatibleWith(const IStatistics & other) const override;

    String getNameForLogs() const override { return "UniqCombined : " + std::to_string(estimateCardinality()); }

private:
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;
};

bool uniqCombinedStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr uniqCombinedStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
