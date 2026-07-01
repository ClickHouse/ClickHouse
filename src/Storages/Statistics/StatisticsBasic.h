#pragma once

#include <Core/Field.h>
#include <Storages/Statistics/Statistics.h>


namespace DB
{

/// `basic` statistics: a compact bundle of single-value summaries derived from a column.
///
/// Depending on the column type, the following sub-statistics are populated:
///   - numeric/temporal columns (anything for which `isValueRepresentedByNumber` is true):
///       `min` and `max` as typed `Field` values
///   - `String` / `FixedString` columns:
///       sum of byte lengths over non-NULL rows (`string_total_bytes`); the average length is
///       `string_total_bytes / non_null_string_count`, which merges trivially across parts.
///   - `Nullable` / `LowCardinality(Nullable)` columns:
///       `null_count` (number of `NULL` rows seen by `build`)
///
/// The same column may contribute to multiple sub-statistics (e.g. a `Nullable(UInt32)` produces
/// both numeric min/max and null count). For sub-statistics not applicable to the column type the
/// corresponding fields stay at their default sentinel values and are not serialized.
///
/// For column types that support sparse serialization, `basic` statistics also count the number of
/// default values (`num_defaults`, exact, via `getNumberOfDefaultRows`). When such an external
/// statistic exists for a column it serves as the authoritative source of the default count for
/// choosing the serialization kind, taking precedence over the counts sampled by `EstimatesBuilder`.
class StatisticsBasic : public IStatistics
{
public:
    StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    std::optional<Float64> estimateLess(const Field & val) const override;
    UInt64 estimateDefaults() const override { return num_defaults; }
    String getNameForLogs() const override;

    bool hasNumericMinMax() const { return tracks_numeric; }
    bool hasStringLengthAvg() const { return tracks_string; }
    bool hasNullCount() const { return tracks_null; }
    bool hasDefaultsCount() const { return tracks_defaults; }

    const Field & getMin() const { return min; }
    const Field & getMax() const { return max; }
    UInt64 getStringTotalBytes() const { return string_total_bytes; }
    /// Average byte length over non-NULL string rows, truncated to an integer. Returns `0` when
    /// no non-NULL string rows were processed; gate on `hasStringLengthAvg()` plus a non-zero
    /// `getStringTotalBytes()` to distinguish "no data" from "all empty strings".
    Int64 getStringLengthAvg() const;
    UInt64 getNullCount() const { return null_count; }
    /// Exact number of default values seen by `build` (only tracked for sparse-supporting types).
    UInt64 getNumDefaults() const { return num_defaults; }
    UInt64 getRowCount() const { return row_count; }

private:
    Field min; /// null Field means "not initialized" (e.g. all values seen so far were NULL)
    Field max; /// null Field means "not initialized"
    UInt64 string_total_bytes = 0;
    UInt64 null_count = 0;
    UInt64 num_defaults = 0;
    UInt64 row_count = 0;

    DataTypePtr data_type; /// stored with LowCardinality and Nullable removed
    bool tracks_numeric = false;
    bool tracks_string = false;
    bool tracks_null = false;
    bool tracks_defaults = false;
};

bool basicStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
