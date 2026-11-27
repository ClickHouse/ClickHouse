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

}
