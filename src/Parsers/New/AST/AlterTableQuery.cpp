#include <Parsers/New/AST/AlterTableQuery.h>

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
PtrTo<AlterTableClause> AlterTableClause::createDrop(bool if_exists, PtrTo<Identifier> identifier)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::DROP, {identifier}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createModify(bool if_exists, PtrTo<TableElementExpr> element)
{
    // TODO: assert(element->getType() == TableElementExpr::ExprType::COLUMN);
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::MODIFY, {element}));
    query->if_exists = if_exists;
    return query;
}

AlterTableClause::AlterTableClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;

    (void)clause_type; // TODO
}

AlterTableQuery::AlterTableQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses)
{
    children.push_back(identifier);
    children.push_back(clauses);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAlterTableAddClause(ClickHouseParser::AlterTableAddClauseContext *ctx)
{
    auto after = ctx->AFTER() ? visit(ctx->nestedIdentifier()).as<PtrTo<Identifier>>() : nullptr;
    return AlterTableClause::createAdd(!!ctx->IF(), visit(ctx->tableColumnDfnt()), after);
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClearClause(ClickHouseParser::AlterTableClearClauseContext *ctx)
{
    return AlterTableClause::createClear(!!ctx->IF(), visit(ctx->nestedIdentifier()), visit(ctx->partitionClause()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableCommentClause(ClickHouseParser::AlterTableCommentClauseContext *ctx)
{
    return AlterTableClause::createComment(!!ctx->IF(), visit(ctx->nestedIdentifier()), Literal::createString(ctx->STRING_LITERAL()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableDropClause(ClickHouseParser::AlterTableDropClauseContext *ctx)
{
    return AlterTableClause::createDrop(!!ctx->IF(), visit(ctx->nestedIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableModifyClause(ClickHouseParser::AlterTableModifyClauseContext *ctx)
{
    return AlterTableClause::createModify(!!ctx->IF(), visit(ctx->tableColumnDfnt()));
}

}
