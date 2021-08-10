#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTIntersectOrExcept.h>


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
    String getName() const { return "IntersectExcept"; }

    Block getCommonHeader(const Blocks & headers) const;

    std::unique_ptr<IInterpreterUnionOrSelectQuery>
    buildCurrentChildInterpreter(const ASTPtr & ast_ptr_);

    void buildQueryPlan(QueryPlan & query_plan);

    ContextPtr context;
    Block result_header;
    std::vector<std::unique_ptr<IInterpreterUnionOrSelectQuery>> nested_interpreters;
    ASTIntersectOrExcept::Modes modes;
};

}
