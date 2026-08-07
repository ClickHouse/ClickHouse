#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndAliases.h>
#include <Backups/BackupOperationInfo.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

/** A struct which will be inserted as row into backup_log table.
  * Contains a record about backup or restore operation.
  */
struct BackupLogElement
{
    std::chrono::system_clock::time_point event_time{};
    Decimal64 event_time_usec{};

    /// Self-contained copies of the loggable fields of BackupOperationInfo (which itself holds a
    /// shared_ptr and an exception_ptr, so it cannot be embedded in a log element). Populate with
    /// BackupLogElement::fromInfo() in the add() callback.
    String id;
    String name_;
    String base_backup_name;
    String query_id;
    BackupStatus status{};
    String error_message;
    UInt64 start_time_us = 0;
    UInt64 end_time_us = 0;
    size_t num_files = 0;
    UInt64 total_size = 0;
    size_t num_entries = 0;
    UInt64 uncompressed_size = 0;
    UInt64 compressed_size = 0;
    size_t num_read_files = 0;
    UInt64 num_read_bytes = 0;
    std::map<String, String> settings;
    std::map<String, String> engine_settings;

    /// Copies the loggable fields out of info into element (leaving out the non-self-contained ones).
    static void fromInfo(BackupLogElement & element, const BackupOperationInfo & info);

    static std::string name() { return "BackupLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class BackupLog : public SystemLog<BackupLogElement>
{
    using SystemLog<BackupLogElement>::SystemLog;
};

}
