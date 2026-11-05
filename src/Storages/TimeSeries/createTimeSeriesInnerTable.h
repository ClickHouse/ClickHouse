#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>

#include <base/UUID.h>


namespace DB
{
class ASTColumns;
class ASTStorage;

/// Creates an inner table using the pre-computed column list. Sets `created` once the inner table is published by this call,
/// also if the call throws afterwards (e.g. from the table's `startup`), and never for a table with this name that existed before.
void createTimeSeriesInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const ASTColumns & inner_columns,
    boost::intrusive_ptr<ASTStorage> inner_storage_def,
    const StorageID & time_series_storage_id,
    UInt64 version,
    ContextPtr context,
    bool & created);

/// Returns the name of an inner table.
String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id, UInt64 version);
String getTimeSeriesInnerTableName(std::string_view inner_table_kind, const StorageID & time_series_storage_id);

}
