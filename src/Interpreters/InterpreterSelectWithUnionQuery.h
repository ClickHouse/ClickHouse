#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class Context;
class InterpreterSelectQuery;
class QueryPlan;

/** Interprets one or multiple SELECT queries inside UNION ALL chain.
  */
class InterpreterSelectWithUnionQuery : public IInterpreter
{
public:
    InterpreterSelectWithUnionQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {});

    ~InterpreterSelectWithUnionQuery() override;

    /// Builds QueryPlan for current query.
    void buildQueryPlan(QueryPlan & query_plan);

    BlockIO execute() override;

    bool ignoreLimits() const override { return options.ignore_limits; }
    bool ignoreQuota() const override { return options.ignore_quota; }

    Block getSampleBlock();

    static Block getSampleBlock(
        const ASTPtr & query_ptr_,
        const Context & context_);

    void ignoreWithTotals();

    ASTPtr getQuery() const { return query_ptr; }

private:
    SelectQueryOptions options;
    ASTPtr query_ptr;
    std::shared_ptr<Context> context;

    std::vector<std::unique_ptr<InterpreterSelectQuery>> nested_interpreters;

    Block result_header;

    size_t max_streams = 1;

    static Block getCommonHeaderForUnion(const Blocks & headers);
};

}
