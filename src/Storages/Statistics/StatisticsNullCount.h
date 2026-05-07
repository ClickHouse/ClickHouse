#pragma once

#include <Storages/Statistics/Statistics.h>

namespace DB
{

/// Tracks NULL value count in Nullable columns for accurate IS NULL / IS NOT NULL selectivity estimation.
class StatisticsNullCount : public IStatistics
{
public:
    explicit StatisticsNullCount(const SingleStatisticsDescription & description);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    String getNameForLogs() const override;

    UInt64 getNullCount() const { return null_count; }

private:
    UInt64 null_count = 0;
};

bool nullCountStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr nullCountStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & /*data_type*/);

}
