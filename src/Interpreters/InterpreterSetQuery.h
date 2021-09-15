#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;
class ASTSetQuery;


/** Change one or several settings for the session or just for the current context.
  */
class InterpreterSetQuery : public IInterpreter
{
public:
    InterpreterSetQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    /** Usual SET query. Set setting for the session.
      */
    BlockIO execute() override;

    /** Set setting for current context (query context).
      * It is used for interpretation of SETTINGS clause in SELECT query.
      */
    void executeForCurrentContext();

private:
    ASTPtr query_ptr;
    Context & context;
};


}
