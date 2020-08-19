#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Return a list of tables or databases meets specified conditions.
  * Interprets a query through replacing it to SELECT query from system.tables or system.databases.
  */
class InterpreterShowTablesQuery : public IInterpreter
{
public:
    InterpreterShowTablesQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;

    String getRewrittenQuery();
};


}
