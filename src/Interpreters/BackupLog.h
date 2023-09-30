#pragma once

#include <Interpreters/SystemLog.h>
#include <Interpreters/BackupInfoElement.h>

namespace DB
{

/** A struct which will be inserted as row into backup_log table.
  * Contains a record about backup or restore operation along with current date and time.
  */
struct BackupLogElement : public BackupInfoElement
{
    using Base = BackupInfoElement;
    BackupLogElement() = default;
    BackupLogElement(BackupOperationInfo info_);
    BackupLogElement(const BackupLogElement &) = default;
    BackupLogElement & operator=(const BackupLogElement &) = default;
    BackupLogElement(BackupLogElement &&) = default;
    BackupLogElement & operator=(BackupLogElement &&) = default;

    std::chrono::system_clock::time_point event_time{};
    Decimal64 event_time_usec{};

    static std::string name() { return "BackupLog"; }
    static NamesAndTypesList getNamesAndTypes();

    void appendToBlock(MutableColumns & columns) const;
};

class BackupLog : public SystemLog<BackupLogElement>
{
    using SystemLog<BackupLogElement>::SystemLog;

public:
    static const char * getDefaultOrderBy() { return "event_date, event_time_microseconds"; }
};

}
