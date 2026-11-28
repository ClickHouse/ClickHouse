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

    void serialize(WriteBuffer & buf, StatisticsFileVersion version) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;
    [[nodiscard]] std::optional<UInt64> getSerializedSize(StatisticsFileVersion version) const override;

    [[nodiscard]] const Field & getMin() const { return min; }
    [[nodiscard]] const Field & getMax() const { return max; }

    /// The statistics read from a file have to be dropped when their bounds are not exact (`V1` stored them as
    /// `Float64`), or have a type other than the one of the values of the column (e.g. the `Float64` bounds of
    /// `V1`, which older versions rewrote unchanged in `V4`), or when they were built for another column type, or when
    /// only one bound is `NaN`.
    [[nodiscard]] bool isUnusable() const override { return is_unusable || StatisticsUtils::hasNaNInOneBoundOnly(min, max); }

    [[nodiscard]] std::optional<Float64> estimateLess(const Field & val) const override;
    [[nodiscard]] String getNameForLogs() const override;
    [[nodiscard]] size_t allocatedBytes() const override;
private:
    Field min; /// null Field means "not initialized"
    Field max; /// null Field means "not initialized"
    UInt64 row_count = 0;

    DataTypePtr data_type;
    /// `data_type` without `LowCardinality`: the type of the values `min` and `max`, serialized with it.
    DataTypePtr value_type;

    bool is_unusable = false;
};

bool minMaxStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
