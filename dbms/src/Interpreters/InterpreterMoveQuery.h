#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;

/** Allows you a in the table.
  */
class InterpreterMoveQuery : public IInterpreter
{
public:
    InterpreterMoveQuery(const ASTPtr & query_ptr_, const Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    const Context & context;
};

}
