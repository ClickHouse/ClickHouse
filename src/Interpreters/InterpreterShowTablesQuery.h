#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Return a list of tables or databases meets specified conditions.
  * Interprets a query through replacing it to SELECT query from system.tables or system.databases.
  */
class InterpreterShowTablesQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowTablesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// We ignore the quota and limits here because execute() will rewrite a show query as a SELECT query and then
    /// the SELECT query will checks the quota and limits.
    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;

    String getRewrittenQuery();
};


}
