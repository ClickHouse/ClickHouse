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
    ManifestListEntry = 2,
    ManifestEntryMetadata = 3,
    ManifestEntry = 4,
};

struct IcebergMetadataLogElement
{
    time_t current_time{};
    String query_id;
    IcebergMetadataLogLevel content_type = IcebergMetadataLogLevel::None;
    String path;
    String filename;
    String metadata_content;

    static std::string name() { return "IcebergMetadataLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

void insertRowToLogTable(
    const ContextPtr & local_context, String row, IcebergMetadataLogLevel row_log_level, const String & file_path, const String & filename);
class IcebergMetadataLog : public SystemLog<IcebergMetadataLogElement>
{
    using SystemLog<IcebergMetadataLogElement>::SystemLog;
};

}
