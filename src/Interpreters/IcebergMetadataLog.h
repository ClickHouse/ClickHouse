#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

enum class IcebergMetadataLogLevel : UInt8
{
    None = 0,
    Metadata = 1,
    ManifestListMetadata = 2,
    ManifestListEntry = 3,
    ManifestFileMetadata = 4,
    ManifestFileEntry = 5,
};

struct IcebergMetadataLogElement
{
    time_t current_time{};
    String query_id;
    IcebergMetadataLogLevel content_type = IcebergMetadataLogLevel::None;
    String path;
    String filename;
    String metadata_content;
    std::optional<UInt64> row_in_file;

    static std::string name() { return "IcebergMetadataLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

IcebergMetadataLogLevel getIcebergMetadataLogLevelFromSettings(const ContextPtr & local_context);

void insertRowToLogTable(
    const ContextPtr & local_context,
    String row,
    IcebergMetadataLogLevel row_log_level,
    const String & file_path,
    const String & filename,
    std::optional<UInt64> row_in_file);
class IcebergMetadataLog : public SystemLog<IcebergMetadataLogElement>
{
    using SystemLog<IcebergMetadataLogElement>::SystemLog;
};

}
