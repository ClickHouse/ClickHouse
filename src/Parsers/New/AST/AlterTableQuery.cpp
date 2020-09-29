#include <Parsers/New/AST/AlterTableQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>
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
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DETACH, {list}));
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

ASTPtr AlterTableClause::convertToOld() const
{
    auto command = std::make_shared<ASTAlterCommand>();

    switch(clause_type)
    {
        case ClauseType::ADD:
            command->type = ASTAlterCommand::ADD_COLUMN;
            command->if_not_exists = if_not_exists;
            // TODO: command->first
            command->col_decl = get(ELEMENT)->convertToOld();
            if (has(AFTER)) command->column = get(AFTER)->convertToOld();
            break;

        case ClauseType::ATTACH:
            command->type = ASTAlterCommand::ATTACH_PARTITION;
            command->partition = get(PARTITION)->convertToOld(); // FIXME: make proper convertion
            if (has(FROM))
            {
                auto table_id = getTableIdentifier(get(FROM)->convertToOld());

                command->from_database = table_id.database_name;
                command->from_table = table_id.table_name;
                command->replace = false;
                command->type = ASTAlterCommand::REPLACE_PARTITION;
            }
            break;

        case ClauseType::CLEAR:
            command->type = ASTAlterCommand::DROP_COLUMN;
            command->if_exists = if_exists;
            command->clear_column = true;
            command->detach = false;
            command->column = get(COLUMN)->convertToOld();
            command->partition = get(IN)->convertToOld(); // FIXME: should be optional?
            break;

        case ClauseType::COMMENT:
            command->type = ASTAlterCommand::COMMENT_COLUMN;
            command->if_exists = if_exists;
            command->column = get(COLUMN)->convertToOld();
            command->comment = get(COMMENT)->convertToOld();
            break;

        case ClauseType::DELETE:
            command->type = ASTAlterCommand::DELETE;
            command->predicate = get(EXPR)->convertToOld();
            break;

        case ClauseType::DETACH:
            command->type = ASTAlterCommand::DROP_PARTITION;
            command->detach = true;
            command->partition = get(PARTITION)->convertToOld();
            break;

        case ClauseType::DROP_COLUMN:
            command->type = ASTAlterCommand::DROP_COLUMN;
            command->if_exists = if_exists;
            command->detach = false;
            command->column = get(COLUMN)->convertToOld();
            break;

        case ClauseType::DROP_PARTITION:
            command->type = ASTAlterCommand::DROP_PARTITION;
            command->partition = get(PARTITION)->convertToOld();
            break;

        case ClauseType::MODIFY:
            command->type = ASTAlterCommand::MODIFY_COLUMN;
            command->if_exists = if_exists;
            command->col_decl = get(ELEMENT)->convertToOld();
            break;

        case ClauseType::ORDER_BY:
            command->type = ASTAlterCommand::MODIFY_ORDER_BY;
            command->order_by = get(EXPR)->convertToOld();
            break;

        case ClauseType::REPLACE:
            command->type = ASTAlterCommand::REPLACE_PARTITION;
            command->replace = true;
            command->partition = get(PARTITION)->convertToOld();
            {
                auto table_id = getTableIdentifier(get(FROM)->convertToOld());
                command->from_database = table_id.database_name;
                command->from_table = table_id.table_name;
            }
            break;
    }

    if (command->col_decl)
        command->children.push_back(command->col_decl);
    if (command->column)
        command->children.push_back(command->column);
    if (command->partition)
        command->children.push_back(command->partition);
    if (command->order_by)
        command->children.push_back(command->order_by);
    if (command->sample_by)
        command->children.push_back(command->sample_by);
    if (command->predicate)
        command->children.push_back(command->predicate);
    if (command->update_assignments)
        command->children.push_back(command->update_assignments);
    if (command->values)
        command->children.push_back(command->values);
    if (command->comment)
        command->children.push_back(command->comment);
    if (command->ttl)
        command->children.push_back(command->ttl);
    if (command->settings_changes)
        command->children.push_back(command->settings_changes);

    return command;
}

AlterTableClause::AlterTableClause(ClauseType type, PtrList exprs) : INode(exprs), clause_type(type)
{
}

AlterTableQuery::AlterTableQuery(PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses) : DDLQuery{identifier, clauses}
{
}

ASTPtr AlterTableQuery::convertToOld() const
{
    auto query = std::make_shared<ASTAlterQuery>();

    {
        auto table_id = getTableIdentifier(get(TABLE)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
    }

    // TODO: query->cluster
    query->set(query->command_list, get(CLAUSES)->convertToOld());

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
    for (auto * clause : ctx->alterTableClause()) list->push(visit(clause));
    return std::make_shared<AlterTableQuery>(visit(ctx->tableIdentifier()), list);
}

}
