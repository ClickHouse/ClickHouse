#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/IAST_fwd.h>

#include <memory>

namespace DB
{

/// Returns single row with explain results
class InterpreterExplainQuery : public IInterpreter, WithContext
{
public:
    InterpreterExplainQuery(const ASTPtr & query_, ContextPtr context_, const SelectQueryOptions & options_);

    ~InterpreterExplainQuery() override;

    BlockIO execute() override;

    static Block getSampleBlock(ASTExplainQuery::ExplainKind kind);

    bool supportsTransactions() const override { return true; }

    /// EXPLAIN ANALYZE executes the inner SELECT, so it must follow the same quota and limit
    /// rules as running that SELECT directly. `executeQuery` charges the query-count quota
    /// (`QUERY_SELECTS` / `QUERIES`) and decides result limits before execution, consulting these
    /// methods. Defer to the inner interpreter's effective flags (e.g. quota-exempt system tables
    /// such as `system.one`). Other EXPLAIN kinds do not execute the inner query and keep the
    /// default behavior.
    bool ignoreQuota() const override;
    bool ignoreLimits() const override;

    bool isExecutableAnalyze() const;

private:
    ASTPtr query;
    SelectQueryOptions options;

    QueryPipeline executeImpl();

    /// The inner SELECT of an EXPLAIN ANALYZE, planned once and cached so that ignoreQuota /
    /// ignoreLimits (called from executeQuery before execution) and executeImpl share a single
    /// planning pass instead of planning twice.
    struct AnalyzedInnerQuery;
    AnalyzedInnerQuery & getAnalyzedInnerQuery() const;

    mutable std::unique_ptr<AnalyzedInnerQuery> analyzed_inner_query;
};


}
