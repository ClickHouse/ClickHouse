#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

class Context;
class InterpreterSelectQuery;
class QueryPlan;

struct NestedInterpreter
{
    ~NestedInterpreter() { }
    enum class Type
    {
        LEAF,
        INTERNAL
    };
    Type type = Type::INTERNAL;
    std::vector<std::shared_ptr<NestedInterpreter>> children;
    std::shared_ptr<InterpreterSelectQuery> interpreter;
    size_t num_distinct_union = 0;
    QueryPlan buildQueryPlan(const std::shared_ptr<Context> & context, const Block & header);
    void ignoreWithTotals();
};

/** Interprets one or multiple SELECT queries inside UNION/UNION ALL/UNION DISTINCT chain.
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

    std::shared_ptr<NestedInterpreter> nested_interpreter;

    Block result_header;

    size_t max_streams = 1;

    static Block getCommonHeaderForUnion(const Blocks & headers);

    static void buildNestedTreeInterpreter(
        const ASTPtr & ast_ptr,
        std::shared_ptr<NestedInterpreter> nested_interpreter_,
        std::vector<std::shared_ptr<InterpreterSelectQuery>> & interpreters,
        int & index);
};

}
