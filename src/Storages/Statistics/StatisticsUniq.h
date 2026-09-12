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

    UInt64 estimateCardinality() const override;
    bool isCompatibleWith(const IStatistics & other) const override;

    String getNameForLogs() const override { return "Uniq : " + std::to_string(estimateCardinality()); }

    /// The `uniq` state is a `UniquesHashSet` whose buffer lives on the heap (not the arena)
    /// and is not observable from here; account its documented ceiling (2^17 * 4 bytes) so the
    /// cache budget cannot be exceeded ~100x by full sets. Overestimating evicts early, which
    /// is the safe direction for a cache.
    size_t memoryUsageBytes() const override { return sizeof(*this) + collector->sizeOfData() + arena->allocatedBytes() + (1ULL << 17) * sizeof(UInt32); }

private:
    std::unique_ptr<Arena> arena;
    AggregateFunctionPtr collector;
    AggregateDataPtr data;

};

bool uniqStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr uniqStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
