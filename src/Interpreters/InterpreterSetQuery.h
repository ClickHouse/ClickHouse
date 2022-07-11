#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTSetQuery;

/** Change one or several settings for the session or just for the current context.
  */
class InterpreterSetQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSetQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    /** Usual SET query. Set setting for the session.
      */
    BlockIO execute() override;

    /** Set setting for current context (query context).
      * It is used for interpretation of SETTINGS clause in SELECT query.
      */
    void executeForCurrentContext();

private:
    ASTPtr query_ptr;
};

}
