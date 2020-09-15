#include <Parsers/New/AST/AlterTableQuery.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/TableElementExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<AlterTableClause> AlterTableClause::createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after)
{
    // TODO: assert(element->getType() == TableElementExpr::ExprType::COLUMN);
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::ADD, {element, after}));
    query->if_not_exists = if_not_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createAttach(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> identifier)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::ATTACH, {list, identifier}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createClear(bool if_exists, PtrTo<Identifier> identifier, PtrTo<PartitionExprList> clause)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::CLEAR, {identifier, clause}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> literal)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::COMMENT, {identifier, literal}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDelete(PtrTo<ColumnExpr> expr)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DELETE, {expr}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDetach(PtrTo<PartitionExprList> list)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DETACH, {list->begin(), list->end()}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDropColumn(bool if_exists, PtrTo<Identifier> identifier)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::DROP_COLUMN, {identifier}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDropPartition(PtrTo<PartitionExprList> list)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DROP_PARTITION, {list->begin(), list->end()}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createModify(bool if_exists, PtrTo<TableElementExpr> element)
{
    // TODO: assert(element->getType() == TableElementExpr::ExprType::COLUMN);
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::MODIFY, {element}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createOrderBy(PtrTo<ColumnExpr> expr)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::ORDER_BY, {expr}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createReplace(PtrTo<PartitionExprList> list, PtrTo<TableIdentifier> identifier)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::REPLACE, {list, identifier}));
}

AlterTableClause::AlterTableClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;

    (void) clause_type; // TODO
}

AlterTableQuery::AlterTableQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses)
{
    children.push_back(identifier);
    children.push_back(clauses);
}

ASTPtr AlterTableQuery::convertToOld() const
{
    auto query = std::make_shared<ASTAlterQuery>();

    // TODO: implement this.

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseAdd(ClickHouseParser::AlterTableClauseAddContext * ctx)
{
    auto after = ctx->AFTER() ? visit(ctx->nestedIdentifier()).as<PtrTo<Identifier>>() : nullptr;
    return AlterTableClause::createAdd(!!ctx->IF(), visit(ctx->tableColumnDfnt()), after);
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseAttach(ClickHouseParser::AlterTableClauseAttachContext *ctx)
{
    auto from = ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr;
    return AlterTableClause::createAttach(visit(ctx->partitionClause()), from);
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseClear(ClickHouseParser::AlterTableClauseClearContext * ctx)
{
    return AlterTableClause::createClear(!!ctx->IF(), visit(ctx->nestedIdentifier()), visit(ctx->partitionClause()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseComment(ClickHouseParser::AlterTableClauseCommentContext * ctx)
{
    return AlterTableClause::createComment(!!ctx->IF(), visit(ctx->nestedIdentifier()), Literal::createString(ctx->STRING_LITERAL()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseDelete(ClickHouseParser::AlterTableClauseDeleteContext *ctx)
{
    return AlterTableClause::createDelete(visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseDetach(ClickHouseParser::AlterTableClauseDetachContext *ctx)
{
    return AlterTableClause::createDetach(visit(ctx->partitionClause()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseDropColumn(ClickHouseParser::AlterTableClauseDropColumnContext * ctx)
{
    return AlterTableClause::createDropColumn(!!ctx->IF(), visit(ctx->nestedIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseDropPartition(ClickHouseParser::AlterTableClauseDropPartitionContext *ctx)
{
    return AlterTableClause::createDropPartition(visit(ctx->partitionClause()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseModify(ClickHouseParser::AlterTableClauseModifyContext * ctx)
{
    return AlterTableClause::createModify(!!ctx->IF(), visit(ctx->tableColumnDfnt()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseOrderBy(ClickHouseParser::AlterTableClauseOrderByContext * ctx)
{
    return AlterTableClause::createOrderBy(visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseReplace(ClickHouseParser::AlterTableClauseReplaceContext *ctx)
{
    return AlterTableClause::createReplace(visit(ctx->partitionClause()), visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext * ctx)
{
    auto list = std::make_shared<List<AlterTableClause>>();
    for (auto * clause : ctx->alterTableClause()) list->append(visit(clause));
    return std::make_shared<AlterTableQuery>(visit(ctx->tableIdentifier()), list);
}

}
