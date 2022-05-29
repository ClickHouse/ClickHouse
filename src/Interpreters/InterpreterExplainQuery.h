#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExplainQuery.h>

namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter, WithContext
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, ContextPtr context_) : WithContext(context_), query(query_) { }

    BlockIO execute() override;

    static Block getSampleBlock(ASTExplainQuery::ExplainKind kind);

    bool supportsTransactions() const override { return true; }

private:
    ASTPtr query;

    QueryPipeline executeImpl();
};


}
