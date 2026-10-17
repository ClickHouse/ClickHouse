#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter, WithContext
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, ContextPtr context_, const SelectQueryOptions & options_)
        : WithContext(context_)
        , query(query_)
        , options(options_)
    {
    }

    BlockIO execute() override;

    static Block getSampleBlock(ASTExplainQuery::ExplainKind kind);

    bool supportsTransactions() const override { return true; }

private:
    ASTPtr query;
    SelectQueryOptions options;

    QueryPipeline executeImpl();
};


}
