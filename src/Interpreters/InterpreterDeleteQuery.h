#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class Context;

class InterpreterDeleteQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    /// Drop table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
