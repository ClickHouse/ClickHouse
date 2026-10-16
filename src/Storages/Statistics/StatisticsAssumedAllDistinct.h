#pragma once

#include <Storages/StatisticsDescription.h>
#include <Storages/Statistics/StatisticsFwd.h>

namespace DB
{

/// Statistics implementation used when a logical uniq-like statistic is materialized by assuming
/// that every non-NULL value is distinct. The logical type remains Uniq/UniqV2; the serialized type
/// is a physical marker so older/default materializations can coexist on disk.
StatisticsPtr createAssumedAllDistinctStatistics(const SingleStatisticsDescription & description, UInt64 cardinality = 0);

bool isAssumedAllDistinctStatistics(const IStatistics & statistics);
StatisticsType getSerializedStatisticsType(const IStatistics & statistics);
bool supportsAssumedAllDistinctStatistics(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
