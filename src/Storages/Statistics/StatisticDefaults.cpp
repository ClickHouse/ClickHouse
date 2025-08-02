#include <Storages/Statistics/StatisticDefaults.h>
#include <IO/WriteHelpers.h>
#include <Columns/IColumn.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnSparse.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

StatisticsDefaults::StatisticsDefaults(const SingleStatisticsDescription & statistics_description)
    : IStatistics(statistics_description)
{
}

void StatisticsDefaults::build(const ColumnPtr & column)
{
    size_t rows = column->size();
    double ratio = column->getRatioOfDefaultRows(ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);

    rows_count += rows;
    num_defaults += static_cast<size_t>(ratio * rows);
}

void StatisticsDefaults::merge(const StatisticsPtr & other_stats)
{
    const StatisticsDefaults * other = typeid_cast<const StatisticsDefaults *>(other_stats.get());
    num_defaults += other->num_defaults;
    rows_count += other->rows_count;
}

void StatisticsDefaults::serialize(WriteBuffer & buf)
{
    writeIntBinary(rows_count, buf);
    writeIntBinary(num_defaults, buf);
}

void StatisticsDefaults::deserialize(ReadBuffer & buf)
{
    readIntBinary(rows_count, buf);
    readIntBinary(num_defaults, buf);
}

UInt64 StatisticsDefaults::estimateDefaults() const
{
    return num_defaults;
}

void defaultsStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & /*data_type*/)
{
}

StatisticsPtr defaultsStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & /*data_type*/)
{
    return std::make_shared<StatisticsDefaults>(description);
}

}
