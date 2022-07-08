#pragma once

#include <Core/Field.h>


namespace DB
{
class IAST;

/// Information about a backup.
struct BackupInfo
{
    String backup_engine_name;
    String id_arg;
    std::vector<Field> args;

    String toString() const;
    static BackupInfo fromString(const String & str);
    static BackupInfo fromAST(const IAST & ast);
};

}
