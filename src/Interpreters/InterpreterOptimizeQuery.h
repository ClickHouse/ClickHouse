#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class AccessRightsElements;

/** Just call method "optimize" for table.
  */
class InterpreterOptimizeQuery : public IInterpreter, WithContext
{
public:
    InterpreterOptimizeQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }

private:
    AccessRightsElements getRequiredAccess() const;

    ASTPtr query_ptr;
};

}
