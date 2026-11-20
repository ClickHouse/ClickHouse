#pragma once

#include <Common/AlignedBuffer.h>
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

private:
    /// Shared by the statistics of all columns of the same type, see `getCollector`.
    AggregateFunctionPtr collector;
    /// The state of `collector`. It does not allocate memory in an arena.
    AlignedBuffer state;
    AggregateDataPtr data;
    /// The size of the serialized state if it is known (after `deserialize`), otherwise 0.
    size_t serialized_size = 0;

    /// Cached result of estimateCardinality + 1. Zero (0) means "not computed yet".
    mutable std::atomic<UInt64> cached_cardinality_plus_one{0};
};

bool uniqV2StatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr uniqV2StatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
