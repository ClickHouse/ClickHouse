#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTUndropQuery.h>

namespace DB
{

class Context;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
class AccessRightsElements;


class InterpreterUndropQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterUndropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    /// Undrop table.
    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    ASTPtr query_ptr;

    /// Binds a hierarchical name (see `DatabaseCatalog`) to the database and the table of the dropped table it denotes.
    void resolveHierarchicalName(ASTUndropQuery & query) const;

    BlockIO executeToTable(ASTUndropQuery & query);
};
}
