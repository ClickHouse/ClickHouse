#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class InterpreterBackupQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterBackupQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
