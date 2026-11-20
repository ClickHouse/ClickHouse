#pragma once

#include <Common/Arena.h>
#include <Storages/Statistics/Statistics.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <atomic>

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
    size_t allocatedBytes() const override;

    std::map<String, StatisticsPtr> getSubcolumnStatistics() const override { return {paths.begin(), paths.end()}; }

private:
    StatisticsUniqV2 * tryGetOrCreatePath(const String & path, const DataTypePtr & path_type);

    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;

    /// Cached result of estimateCardinality + 1. Zero (0) means "not computed yet".
    mutable std::atomic<UInt64> cached_cardinality_plus_one{0};

    /// For a `JSON` column, its type and the statistics of each path whose type supports them.
    DataTypePtr json_type;
    std::map<String, std::shared_ptr<StatisticsUniqV2>> paths;
};

bool uniqV2StatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr uniqV2StatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
