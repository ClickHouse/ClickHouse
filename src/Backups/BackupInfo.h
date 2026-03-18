#pragma once

#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;

/// Information about a backup.
struct BackupInfo
{
    String backup_engine_name;
    String id_arg;
    std::vector<Field> args;

    String toString() const;
    static BackupInfo fromString(const String & str);

    ASTPtr toAST() const;
    static BackupInfo fromAST(const IAST & ast);

    String toStringForLogging() const;

    void copyS3CredentialsTo(BackupInfo & dest) const;
};

}
