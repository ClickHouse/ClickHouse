#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class AccessRightsElements;

class InterpreterDropAccessEntityQuery : public IInterpreter
{
public:
    InterpreterDropAccessEntityQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;
    Context & context;
};
}
