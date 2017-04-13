#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Allow to either drop table with all its data (DROP), or remove information about table (just forget) from server (DETACH).
  */
class InterpreterDropQuery : public IInterpreter
{
public:
    InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_);

    /// Drop table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;

    BlockIO executeOnServer(ASTDropQuery & drop);
    BlockIO executeOnCluster(ASTDropQuery & drop);
};


}
