#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ColumnsDescription;
struct ColumnDescription;
struct TimeSeriesSettings;

/// Checks the types of columns of a TimeSeries table.
class TimeSeriesColumnsValidator
{
public:
    using TargetKind = ViewTarget::Kind;
    explicit TimeSeriesColumnsValidator(const StorageID & storage_id_) : storage_id(storage_id_) {}

    /// Checks the columns of a TimeSeries table and throws an exception if some of the required columns don't exist or have illegal types.
    void validateColumns(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const;

    /// Checks columns of a target table that a TimeSeries table is going to use.
    /// Throws an exception if some of the required columns don't exist or have illegal types.
    void validateTargetColumns(TargetKind kind, const ColumnsDescription & target_columns, const TimeSeriesSettings & time_series_settings) const;

    /// Each of the following functions validates a specific column type.
    void validateColumnForID(const ColumnDescription & column, bool check_default = true) const;
    void validateColumnForTimestamp(const ColumnDescription & column) const;
    void validateColumnForTimestamp(const ColumnDescription & column, UInt32 & out_scale) const;
    void validateColumnForValue(const ColumnDescription & column) const;

    void validateColumnForMetricName(const ColumnDescription & column) const;
    void validateColumnForTagValue(const ColumnDescription & column) const;
    void validateColumnForTagsMap(const ColumnDescription & column) const;

    void validateColumnForMetricFamilyName(const ColumnDescription & column) const;
    void validateColumnForType(const ColumnDescription & column) const;
    void validateColumnForUnit(const ColumnDescription & column) const;
    void validateColumnForHelp(const ColumnDescription & column) const;

private:
    StorageID storage_id;
};

}
