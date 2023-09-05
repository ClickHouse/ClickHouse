#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Backups/BackupOperationInfo.h>

namespace DB
{

/** A struct which will be inserted as row into system.backups table.
  * Contains a record about backup or restore operation.
  */
struct BackupInfoElement
{
    BackupInfoElement() = default;
    BackupInfoElement(BackupOperationInfo info_);
    BackupInfoElement(const BackupInfoElement &) = default;
    BackupInfoElement & operator=(const BackupInfoElement &) = default;
    BackupInfoElement(BackupInfoElement &&) = default;
    BackupInfoElement & operator=(BackupInfoElement &&) = default;

    BackupOperationInfo info{};

    static std::string name() { return "Backups"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    static const char * getCustomColumnList() { return nullptr; }

    void appendToBlock(MutableColumns & columns, size_t first_column_index = 0) const;
};

}
