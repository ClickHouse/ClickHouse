#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTCreateQuery;
class ColumnsDescription;
struct TimeSeriesSettings;

/// Generates inner tables for the TimeSeries table engine.
class TimeSeriesInnerTablesCreator : public WithContext
{
public:
    /// Constructor stores references to arguments `time_series_columns_` and `time_series_settings_` (it's unnecessary to copy them).
    TimeSeriesInnerTablesCreator(ContextPtr context_,
                                 StorageID time_series_storage_id_,
                                 std::reference_wrapper<const ColumnsDescription> time_series_columns_,
                                 std::reference_wrapper<const TimeSeriesSettings> time_series_settings_);

    ~TimeSeriesInnerTablesCreator();

    /// Returns a column description of an inner table.
    ColumnsDescription getInnerTableColumnsDescription(ViewTarget::Kind inner_table_kind) const;

    /// Returns a StorageID of an inner table.
    StorageID getInnerTableID(ViewTarget::Kind inner_table_kind, const UUID & inner_table_uuid) const;

    /// Generates a CREATE TABLE query for an inner table.
    std::shared_ptr<ASTCreateQuery> getInnerTableCreateQuery(ViewTarget::Kind inner_table_kind,
                                                             const UUID & inner_table_uuid,
                                                             const std::shared_ptr<ASTStorage> & inner_storage_def) const;

    /// Creates an inner table.
    StorageID createInnerTable(ViewTarget::Kind inner_table_kind,
                               const UUID & inner_table_uuid,
                               const std::shared_ptr<ASTStorage> & inner_storage_def) const;

private:
    const StorageID time_series_storage_id;
    const ColumnsDescription & time_series_columns;
    const TimeSeriesSettings & time_series_settings;
};

}
