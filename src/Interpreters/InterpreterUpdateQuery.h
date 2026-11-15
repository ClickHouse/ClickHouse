#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Allows to do lightweight updates on a MergeTree family tables.
class InterpreterUpdateQuery : public IInterpreter, WithContext
{
public:
    InterpreterUpdateQuery(ASTPtr query_ptr_, ContextPtr context_);
    BlockIO execute() override;
    bool supportsTransactions() const override { return false; }

private:
    ASTPtr query_ptr;
};

}
