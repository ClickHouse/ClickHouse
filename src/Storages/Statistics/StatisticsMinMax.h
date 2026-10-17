#pragma once

#include <Core/Field.h>
#include <Storages/Statistics/Statistics.h>


namespace DB
{

class StatisticsMinMax : public IStatistics
{
public:
    StatisticsMinMax(const SingleStatisticsDescription & statistics_description, const DataTypePtr & data_type_);

    /// For tests only: construct with known min, max, row_count (no data_type needed for estimation).
    StatisticsMinMax(Field min_, Field max_, UInt64 row_count_);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    [[nodiscard]] const Field & getMin() const { return min; }
    [[nodiscard]] const Field & getMax() const { return max; }

    [[nodiscard]] std::optional<Float64> estimateLess(const Field & val) const override;
    [[nodiscard]] String getNameForLogs() const override;
    [[nodiscard]] size_t allocatedBytes() const override;
private:
    Field min; /// null Field means "not initialized"
    Field max; /// null Field means "not initialized"
    UInt64 row_count = 0;

    DataTypePtr data_type;
};

bool minMaxStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
