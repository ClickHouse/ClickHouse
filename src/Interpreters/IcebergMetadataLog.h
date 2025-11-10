#pragma once

#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>
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

/// Here `get_row` function is used instead `row` string to calculate string only when required.
/// Inside `insertRowToLogTable` code can exit immediately after `iceberg_metadata_log_level` setting check.
void insertRowToLogTable(
    const ContextPtr & local_context,
    std::function<String()> get_row,
    IcebergMetadataLogLevel row_log_level,
    const String & table_path,
    const String & file_path,
    std::optional<UInt64> row_in_file,
    std::optional<Iceberg::PruningReturnStatus> pruning_status);

class IcebergMetadataLog : public SystemLog<IcebergMetadataLogElement>
{
    using SystemLog<IcebergMetadataLogElement>::SystemLog;
};

}
