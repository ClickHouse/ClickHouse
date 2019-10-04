#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;


/** Returns a single item containing a statement which could be used to create a specified role.
  */
class InterpreterShowCreateUserQuery : public IInterpreter
{
public:
    InterpreterShowCreateUserQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    const Context & context;

    BlockInputStreamPtr executeImpl();
    ASTPtr getCreateUserQuery() const;
};


}
