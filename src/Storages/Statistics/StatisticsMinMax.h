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

    /// Whether the bounds were read from a file of `V1`, as `Float64`, and could not be converted to the
    /// type of the column. Such statistics have to be dropped: they cannot be written in the current format.
    [[nodiscard]] bool hasUnconvertibleLegacyBounds() const { return has_unconvertible_legacy_bounds; }

    [[nodiscard]] std::optional<Float64> estimateLess(const Field & val) const override;
    [[nodiscard]] String getNameForLogs() const override;
    [[nodiscard]] size_t allocatedBytes() const override;
private:
    /// Converts the `Float64` bounds of `V1` to the type of the column, widening them to the nearest values
    /// of the type, so that they still contain all values of the column.
    void convertLegacyBounds(Float64 min_value, Float64 max_value);

    Field min; /// null Field means "not initialized"
    Field max; /// null Field means "not initialized"
    UInt64 row_count = 0;

    DataTypePtr data_type;
    /// `data_type` without `LowCardinality`: the type of the values `min` and `max`, serialized with it.
    DataTypePtr value_type;

    bool has_unconvertible_legacy_bounds = false;
};

bool minMaxStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
