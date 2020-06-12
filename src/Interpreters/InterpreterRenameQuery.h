#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;
class AccessRightsElements;

/// To avoid deadlocks, we must acquire locks for tables in same order in any different RENAMES.
struct UniqueTableName
{
    String database_name;
    String table_name;

    UniqueTableName(const String & database_name_, const String & table_name_)
            : database_name(database_name_), table_name(table_name_) {}

    bool operator< (const UniqueTableName & rhs) const
    {
        return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
    }
};

using TableGuards = std::map<UniqueTableName, std::unique_ptr<DDLGuard>>;

/** Rename one table
  *  or rename many tables at once.
  */
class InterpreterRenameQuery : public IInterpreter
{
public:
    InterpreterRenameQuery(const ASTPtr & query_ptr_, Context & context_);
    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;
    Context & context;
};

}
