#pragma once

#include <Storages/Statistics/Statistics.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class StatisticsDefaults : public IStatistics
{
public:
    explicit StatisticsDefaults(const SingleStatisticsDescription & statistics_description);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    UInt64 estimateDefaults() const override;
    UInt64 rowsCount() const { return rows_count; }

private:
    UInt64 num_defaults = 0;
    UInt64 rows_count = 0;
};

bool defaultsStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr defaultsStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
