#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
class ASTSetQuery;
using ASTPtr = std::shared_ptr<IAST>;


/** Change one or several settings, for session or globally, or just for current context.
  */
class InterpreterSetQuery : public IInterpreter
{
public:
    InterpreterSetQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    /** Usual SET query. Set setting for session or globally (if GLOBAL was specified).
      */
    BlockIO execute() override;

    void checkAccess(const ASTSetQuery & ast);

    /** Set setting for current context (query context).
      * It is used for interpretation of SETTINGS clause in SELECT query.
      */
    void executeForCurrentContext();

private:
    ASTPtr query_ptr;
    Context & context;

    void executeImpl(const ASTSetQuery & ast, Context & target);
};


}
