#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
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
    BackupLogElement() = default;
    explicit BackupLogElement(BackupOperationInfo info_);
    BackupLogElement(const BackupLogElement &) = default;
    BackupLogElement & operator=(const BackupLogElement &) = default;
    BackupLogElement(BackupLogElement &&) = default;
    BackupLogElement & operator=(BackupLogElement &&) = default;

    std::chrono::system_clock::time_point event_time{};
    Decimal64 event_time_usec{};
    BackupOperationInfo info{}; /// NOLINT(bugprone-throw-keyword-missing)

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
