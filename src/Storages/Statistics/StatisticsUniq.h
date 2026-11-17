#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

class StatisticsUniq : public IStatistics
{
public:
    StatisticsUniq(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
    ~StatisticsUniq() override;

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    [[nodiscard]] UInt64 estimateCardinality() const override;
    [[nodiscard]] bool isCompatibleWith(const IStatistics & other) const override;

    [[nodiscard]] String getNameForLogs() const override { return "Uniq : " + std::to_string(estimateCardinality()); }
    [[nodiscard]] size_t allocatedBytes() const override;
private:
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;

};

bool uniqStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr uniqStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
