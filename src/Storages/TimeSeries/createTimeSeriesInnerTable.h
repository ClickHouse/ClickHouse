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
/// `version` is the version of the TimeSeries table (see TimeSeriesVersion.h), it affects the name of the inner table.
void createTimeSeriesInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const ASTColumns & inner_columns,
    boost::intrusive_ptr<ASTStorage> inner_storage_def,
    const StorageID & time_series_storage_id,
    UInt64 version,
    ContextPtr context);

/// Returns the name of a target kind used in the names of inner tables and in the paths inside backups,
/// for example "samples" or "tags". The name depends on the version of the TimeSeries table:
/// the "metric families" target is named "metrics" in the versions before TimeSeriesVersion::FIRST_WITH_METRIC_FAMILIES_NAME.
String getTimeSeriesTargetKindName(ViewTarget::Kind target_kind, UInt64 version);

/// Returns the name of an inner table.
String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id, UInt64 version);
String getTimeSeriesInnerTableName(std::string_view inner_table_kind, const StorageID & time_series_storage_id);

}
