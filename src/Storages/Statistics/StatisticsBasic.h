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
///   - any supported column type:
///       `default_count` (number of rows equal to the *type's* default value seen by `build`).
///       "Default" here is the type-intrinsic default (`IColumn::isDefaultAt` / `IDataType::getDefault`),
///       not the column's DDL `DEFAULT` expression. For a `Nullable` / `LowCardinality(Nullable)`
///       column the type default is `NULL` (`ColumnNullable::isDefaultAt` == `isNullAt`), so
///       `default_count` is exactly the number of `NULL` rows and drives `IS NULL` estimation.
///       `Variant` and `Dynamic` columns also have a `NULL` type default (their `isDefaultAt`
///       returns true for `NULL_DISCRIMINATOR` rows), so they likewise expose `default_count` as
///       a null count. For any other non-`Nullable` column it is the number of type-default rows
///       (`0`, `''`, ...) and drives equality-to-default estimation (`col = 0`, `col = ''`).
///
/// The same column may contribute to multiple sub-statistics (e.g. a `Nullable(UInt32)` produces
/// both numeric min/max and a default/NULL count). For sub-statistics not applicable to the column
/// type the corresponding fields stay at their default sentinel values and are not serialized.
class StatisticsBasic : public IStatistics
{
public:
    StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    [[nodiscard]] std::optional<Float64> estimateLess(const Field & val) const override;
    [[nodiscard]] std::optional<Float64> estimateEqual(const Field & val) const override;
    [[nodiscard]] String getNameForLogs() const override;
    [[nodiscard]] size_t allocatedBytes() const override;

    [[nodiscard]] bool isCompatibleWith(const IStatistics & other) const override;
    void copyStructureFrom(const IStatistics & other) override;

    [[nodiscard]] bool hasNumericMinMax() const { return tracks_numeric; }
    [[nodiscard]] bool hasStringLengthAvg() const { return tracks_string; }
    [[nodiscard]] bool hasNullCount() const { return is_nullable && has_default_count; }
    [[nodiscard]] bool hasDefaultCount() const { return has_default_count; }

    [[nodiscard]] const Field & getMin() const { return min; }
    [[nodiscard]] const Field & getMax() const { return max; }
    [[nodiscard]] UInt64 getStringTotalBytes() const { return string_total_bytes; }
    [[nodiscard]] Int64 getStringLengthAvg() const;
    [[nodiscard]] UInt64 getNullCount() const { return (is_nullable && has_default_count) ? default_count : 0; }
    [[nodiscard]] UInt64 getDefaultCount() const { return default_count; }
    [[nodiscard]] UInt64 getRowCount() const { return row_count; }

private:
    Field min; /// null Field means "not initialized" (e.g. all values seen so far were NULL)
    Field max; /// null Field means "not initialized"
    UInt64 string_total_bytes = 0;
    UInt64 default_count = 0; /// rows equal to the type default; == NULL count for a Nullable column
    UInt64 row_count = 0;

    DataTypePtr data_type; /// stored with LowCardinality and Nullable removed
    /// Column-level default: the Field produced by `IColumn::insertDefault()`.
    Field column_default_field;
    bool tracks_numeric = false;
    bool tracks_string = false;
    bool is_nullable = false;    /// column's type default is NULL (Nullable, LowCardinality(Nullable), Variant, Dynamic, ...)
    bool has_default_count = true; /// true unless deserialized from older code that did not write a default-value count
};

bool basicStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
