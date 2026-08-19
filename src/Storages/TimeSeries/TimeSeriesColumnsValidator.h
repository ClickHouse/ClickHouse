#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ColumnsDescription;
struct ColumnDescription;
struct ColumnWithTypeAndName;
struct TimeSeriesSettings;

/// Checks the types of columns of a TimeSeries table.
class TimeSeriesColumnsValidator
{
public:
    /// Constructor stores a reference to argument `time_series_settings_` (it's unnecessary to copy it).
    TimeSeriesColumnsValidator(StorageID time_series_storage_id_,
                               std::reference_wrapper<const TimeSeriesSettings> time_series_settings_);

    /// Checks the columns of a TimeSeries table and throws an exception if some of the required columns don't exist or have illegal types.
    void validateColumns(const ColumnsDescription & columns) const;

    /// Checks columns of a target table that a TimeSeries table is going to use.
    /// Throws an exception if some of the required columns don't exist or have illegal types.
    void validateTargetColumns(ViewTarget::Kind target_kind, const StorageID & target_table_id, const ColumnsDescription & target_columns) const;

    /// Each of the following functions validates a specific column type.
    void validateColumnForID(const ColumnDescription & column, bool check_default = true) const;
    void validateColumnForTimestamp(const ColumnDescription & column) const;
    void validateColumnForTimestamp(const ColumnDescription & column, UInt32 & out_scale) const;
    void validateColumnForValue(const ColumnDescription & column) const;

    void validateColumnForMetricName(const ColumnDescription & column) const;
    void validateColumnForMetricName(const ColumnWithTypeAndName & column) const;
    void validateColumnForTagValue(const ColumnDescription & column) const;
    void validateColumnForTagValue(const ColumnWithTypeAndName & column) const;
    void validateColumnForTagsMap(const ColumnDescription & column) const;
    void validateColumnForTagsMap(const ColumnWithTypeAndName & column) const;

    void validateColumnForMetricFamilyName(const ColumnDescription & column) const;
    void validateColumnForType(const ColumnDescription & column) const;
    void validateColumnForUnit(const ColumnDescription & column) const;
    void validateColumnForHelp(const ColumnDescription & column) const;

private:
    void validateColumnsImpl(const ColumnsDescription & columns) const;
    void validateTargetColumnsImpl(ViewTarget::Kind target_kind, const ColumnsDescription & target_columns) const;

    const StorageID time_series_storage_id;
    const TimeSeriesSettings & time_series_settings;
};

}
