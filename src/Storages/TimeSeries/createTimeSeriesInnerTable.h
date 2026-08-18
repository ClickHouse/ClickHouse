#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>

#include <base/UUID.h>


namespace DB
{
class ASTColumns;
class ASTStorage;

/// Creates an inner table using the pre-computed column list.
void createTimeSeriesInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const ASTColumns & inner_columns,
    boost::intrusive_ptr<ASTStorage> inner_storage_def,
    const StorageID & time_series_storage_id,
    ContextPtr context);

/// Returns a StorageID of an inner table.
String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id);
String getTimeSeriesInnerTableName(std::string_view inner_table_kind, const StorageID & time_series_storage_id);

/// Creates the inner materialized view which copies the `id`, `timestamp`, `value` columns as is
/// from the samples table into the recent samples table.
void createTimeSeriesRecentSamplesMV(
    const StorageID & samples_table_id,
    const StorageID & recent_samples_table_id,
    const StorageID & time_series_storage_id,
    ContextPtr context);

/// Returns the name of the inner materialized view created by `createTimeSeriesRecentSamplesMV`.
String getTimeSeriesRecentSamplesMVName(const StorageID & time_series_storage_id);

}
