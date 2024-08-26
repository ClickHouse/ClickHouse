#pragma once

#include <Core/UUID.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCheckGrantQuery;
struct User;
struct Role;

class InterpreterCheckGrantQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCheckGrantQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
