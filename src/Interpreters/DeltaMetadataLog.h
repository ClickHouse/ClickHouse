#pragma once

#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct DeltaMetadataLogElement
{
    time_t current_time{};
    String query_id;
    String table_path;
    String file_path;
    String metadata_content;

    static std::string name() { return "DeltaMetadataLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

void insertDeltaRowToLogTable(
    const ContextPtr & local_context,
    String row,
    const String & table_path,
    const String & file_path);

class DeltaMetadataLog : public SystemLog<DeltaMetadataLogElement>
{
    using SystemLog<DeltaMetadataLogElement>::SystemLog;
};

}
