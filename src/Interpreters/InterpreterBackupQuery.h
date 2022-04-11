#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class InterpreterBackupQuery : public IInterpreter
{
public:
    InterpreterBackupQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
};
}
