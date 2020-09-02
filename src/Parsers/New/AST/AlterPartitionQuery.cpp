#include <Parsers/New/AST/AlterPartitionQuery.h>

#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<AlterPartitionClause> AlterPartitionClause::createDetach(PtrTo<PartitionExprList> list)
{
    return PtrTo<AlterPartitionClause>(new AlterPartitionClause(ClauseType::DETACH, {list->begin(), list->end()}));
}

// static
PtrTo<AlterPartitionClause> AlterPartitionClause::createDrop(PtrTo<PartitionExprList> list)
{
    return PtrTo<AlterPartitionClause>(new AlterPartitionClause(ClauseType::DROP, {list->begin(), list->end()}));
}

AlterPartitionClause::AlterPartitionClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;

    (void)clause_type; // TODO
}

AlterPartitionQuery::AlterPartitionQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterPartitionClause>> clauses)
{
    children.push_back(identifier);
    children.push_back(clauses);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAlterPartitionDetachClause(ClickHouseParser::AlterPartitionDetachClauseContext *ctx)
{
    return AlterPartitionClause::createDetach(visit(ctx->partitionClause()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterPartitionDropClause(ClickHouseParser::AlterPartitionDropClauseContext *ctx)
{
    return AlterPartitionClause::createDrop(visit(ctx->partitionClause()));
}

}
