#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

namespace DB
{

class Context;
class InterpreterSelectQuery;
class QueryPlan;

class InterpreterIntersectOrExcept : public IInterpreter
{
public:
    InterpreterIntersectOrExcept(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

private:
    String getName() const { return is_except ? "EXCEPT" : "INTERSECT"; }

    Block getCommonHeader(const Blocks & headers) const;

    std::unique_ptr<IInterpreterUnionOrSelectQuery>
    buildCurrentChildInterpreter(const ASTPtr & ast_ptr_);

    void buildQueryPlan(QueryPlan & query_plan);

    ContextPtr context;
    bool is_except;
    Block result_header;
    std::vector<std::unique_ptr<IInterpreterUnionOrSelectQuery>> nested_interpreters;
};

}
