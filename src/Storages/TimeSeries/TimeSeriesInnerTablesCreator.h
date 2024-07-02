#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTCreateQuery;
class ColumnsDescription;
struct TimeSeriesSettings;

/// Generates inner tables for the TimeSeries table engine.
class TimeSeriesInnerTablesCreator
{
public:
    using TargetKind = ViewTarget::Kind;
    explicit TimeSeriesInnerTablesCreator(const StorageID & time_series_storage_id_) : time_series_storage_id(time_series_storage_id_) {}

    /// Returns a StorageID of an inner table.
    StorageID getInnerTableId(TargetKind kind, const ViewTarget * target_info) const;

    /// Generates a CREATE TABLE query for an inner table.
    std::shared_ptr<ASTCreateQuery> generateCreateQueryForInnerTable(TargetKind kind, const ViewTarget * target_info,
                                                                     const ColumnsDescription & time_series_columns,
                                                                     const TimeSeriesSettings & time_series_settings) const;

    /// Creates an inner table.
    StorageID createInnerTable(TargetKind kind,
                               const ViewTarget * target_info,
                               const ContextPtr & context,
                               const ColumnsDescription & time_series_columns,
                               const TimeSeriesSettings & time_series_settings) const;

private:
    StorageID time_series_storage_id;
};

}
