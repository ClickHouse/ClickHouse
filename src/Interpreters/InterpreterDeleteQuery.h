#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/** Allows you do lightweight deletion on a MergeTree family table.
  */
class InterpreterDeleteQuery : public IInterpreter, WithContext
{
public:
    InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

    bool supportsTransactions() const override { return true; }

private:
    ASTPtr query_ptr;
};

}
