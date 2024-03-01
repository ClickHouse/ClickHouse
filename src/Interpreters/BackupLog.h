#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Backups/BackupOperationInfo.h>

namespace DB
{

/** A struct which will be inserted as row into backup_log table.
  * Contains a record about backup or restore operation.
  */
struct BackupLogElement
{
    BackupLogElement() = default;
    BackupLogElement(BackupOperationInfo info_);
    BackupLogElement(const BackupLogElement &) = default;
    BackupLogElement & operator=(const BackupLogElement &) = default;
    BackupLogElement(BackupLogElement &&) = default;
    BackupLogElement & operator=(BackupLogElement &&) = default;

    std::chrono::system_clock::time_point event_time{};
    Decimal64 event_time_usec{};
    BackupOperationInfo info{};

    static std::string name() { return "BackupLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class BackupLog : public SystemLog<BackupLogElement>
{
    using SystemLog<BackupLogElement>::SystemLog;

public:
    static const char * getDefaultOrderBy() { return "event_date, event_time_microseconds"; }
};

}
