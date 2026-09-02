#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>

namespace DB
{

struct IcebergMetadataLogElement
{
    time_t current_time{};
    String query_id;
    IcebergMetadataLogLevel content_type = IcebergMetadataLogLevel::None;
    String table_path;
    String file_path;
    String metadata_content;
    std::optional<UInt64> row_in_file;
    std::optional<Iceberg::PruningReturnStatus> pruning_status;

    static std::string name() { return "IcebergMetadataLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

/// Returns the value of the query-level setting `iceberg_metadata_log_level`.
IcebergMetadataLogLevel getIcebergMetadataLogLevel(const ContextPtr & local_context);

void insertRowToLogTableImpl(
    const ContextPtr & local_context,
    String row,
    IcebergMetadataLogLevel row_log_level,
    const String & table_path,
    const Iceberg::IcebergPathFromMetadata & file_path,
    std::optional<UInt64> row_in_file,
    std::optional<Iceberg::PruningReturnStatus> pruning_status);

/// Inserts a row into `system.iceberg_metadata_log` if the query-level setting
/// `iceberg_metadata_log_level` admits `row_log_level`.
template <typename GetRow>
requires std::is_invocable_r_v<String, GetRow>
void insertRowToLogTable(
    const ContextPtr & local_context,
    GetRow && get_row,
    IcebergMetadataLogLevel row_log_level,
    const String & table_path,
    const Iceberg::IcebergPathFromMetadata & file_path,
    std::optional<UInt64> row_in_file,
    std::optional<Iceberg::PruningReturnStatus> pruning_status)
{
    if (getIcebergMetadataLogLevel(local_context) < row_log_level)
        return;
    insertRowToLogTableImpl(local_context, get_row(), row_log_level, table_path, file_path, row_in_file, pruning_status);
}

class IcebergMetadataLog : public SystemLog<IcebergMetadataLogElement>
{
    using SystemLog<IcebergMetadataLogElement>::SystemLog;
};

}
