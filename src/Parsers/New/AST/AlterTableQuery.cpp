#include <Parsers/New/AST/AlterTableQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/TableElementExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

PartitionClause::PartitionClause(PtrTo<Literal> id) : PartitionClause(ClauseType::ID, {id})
{
}

PartitionClause::PartitionClause(PtrTo<List<Literal>> list) : PartitionClause(ClauseType::LIST, {list})
{
}

PartitionClause::PartitionClause(ClauseType type, PtrList exprs) : INode(exprs), clause_type(type)
{
}

ASTPtr PartitionClause::convertToOld() const
{
    auto partition = std::make_shared<ASTPartition>();

    switch(clause_type)
    {
        case ClauseType::ID:
            partition->id = get(ID)->as<StringLiteral>()->as<String>();
            break;
        case ClauseType::LIST:
            {
                auto tuple = std::make_shared<ASTFunction>();

                tuple->name = "tuple";
                tuple->arguments = std::make_shared<ASTExpressionList>();
                for (const auto & child : get(LIST)->as<List<Literal> &>())
                    tuple->arguments->children.push_back(child->convertToOld());
                tuple->children.push_back(tuple->arguments);

                partition->value = tuple;
                partition->children.push_back(partition->value);
                partition->fields_count = get(LIST)->as<List<Literal>>()->size();
                partition->fields_str = get(LIST)->toString();
            }
            break;
    }

    return partition;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createAdd(bool if_not_exists, PtrTo<TableElementExpr> element, PtrTo<Identifier> after)
{
    // TODO: assert(element->getType() == TableElementExpr::ExprType::COLUMN);
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::ADD, {element, after}));
    query->if_not_exists = if_not_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createAttach(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> from)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::ATTACH, {clause, from}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createClear(bool if_exists, PtrTo<Identifier> identifier, PtrTo<PartitionClause> in)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::CLEAR, {identifier, in}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createCodec(bool if_exists, PtrTo<Identifier> identifier, PtrTo<CodecExpr> codec)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::CODEC, {identifier, codec}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createComment(bool if_exists, PtrTo<Identifier> identifier, PtrTo<StringLiteral> comment)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::COMMENT, {identifier, comment}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDelete(PtrTo<ColumnExpr> expr)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DELETE, {expr}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDetach(PtrTo<PartitionClause> clause)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DETACH, {clause}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDropColumn(bool if_exists, PtrTo<Identifier> identifier)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::DROP_COLUMN, {identifier}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createDropPartition(PtrTo<PartitionClause> clause)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::DROP_PARTITION, {clause}));
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
PtrTo<AlterTableClause> AlterTableClause::createRemove(bool if_exists, PtrTo<Identifier> identifier, TableColumnPropertyType type)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::REMOVE, {identifier}));
    query->if_exists = if_exists;
    query->property_type = type;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createRemoveTTL()
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::REMOVE_TTL, {}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createRename(bool if_exists, PtrTo<Identifier> identifier, PtrTo<Identifier> to)
{
    PtrTo<AlterTableClause> query(new AlterTableClause(ClauseType::RENAME, {identifier, to}));
    query->if_exists = if_exists;
    return query;
}

// static
PtrTo<AlterTableClause> AlterTableClause::createReplace(PtrTo<PartitionClause> clause, PtrTo<TableIdentifier> from)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::REPLACE, {clause, from}));
}

// static
PtrTo<AlterTableClause> AlterTableClause::createTTL(PtrTo<TTLClause> clause)
{
    return PtrTo<AlterTableClause>(new AlterTableClause(ClauseType::TTL, {clause}));
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
            command->partition = get(PARTITION)->convertToOld();

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
            if (has(IN)) command->partition = get(IN)->convertToOld();
            break;

        case ClauseType::CODEC:
            command->type = ASTAlterCommand::MODIFY_COLUMN;
            command->if_exists = if_exists;

            {
                auto column = std::make_shared<ASTColumnDeclaration>();
                column->name = get(COLUMN)->toString();
                column->codec = get(CODEC)->convertToOld();

                command->col_decl = column;
            }
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

        case ClauseType::REMOVE:
            command->type = ASTAlterCommand::MODIFY_COLUMN;
            command->if_exists = if_exists;
            command->col_decl = get(ELEMENT)->convertToOld();
            switch(property_type)
            {
                case TableColumnPropertyType::ALIAS:
                    command->remove_property = "ALIAS";
                    break;
                case TableColumnPropertyType::CODEC:
                    command->remove_property = "CODEC";
                    break;
                case TableColumnPropertyType::COMMENT:
                    command->remove_property = "COMMENT";
                    break;
                case TableColumnPropertyType::DEFAULT:
                    command->remove_property = "DEFAULT";
                    break;
                case TableColumnPropertyType::MATERIALIZED:
                    command->remove_property = "MATERIALIZED";
                    break;
                case TableColumnPropertyType::TTL:
                    command->remove_property = "TTL";
                    break;
            }
            break;

        case ClauseType::REMOVE_TTL:
            command->type = ASTAlterCommand::REMOVE_TTL;
            break;

        case ClauseType::RENAME:
            command->type = ASTAlterCommand::RENAME_COLUMN;
            command->column = get(COLUMN)->convertToOld();
            command->rename_to = get(TO)->convertToOld();
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

        case ClauseType::TTL:
            command->type = ASTAlterCommand::MODIFY_TTL;
            command->ttl = get(CLAUSE)->convertToOld();
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

AlterTableQuery::AlterTableQuery(PtrTo<ClusterClause> cluster, PtrTo<TableIdentifier> identifier, PtrTo<List<AlterTableClause>> clauses)
    : DDLQuery(cluster, {identifier, clauses})
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

    query->cluster = cluster_name;

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
    auto partition = ctx->partitionClause() ? visit(ctx->partitionClause()).as<PtrTo<PartitionClause>>() : nullptr;
    return AlterTableClause::createClear(!!ctx->IF(), visit(ctx->nestedIdentifier()), partition);
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

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseModifyCodec(ClickHouseParser::AlterTableClauseModifyCodecContext * ctx)
{
    return AlterTableClause::createCodec(!!ctx->IF(), visit(ctx->nestedIdentifier()), visit(ctx->codecExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseModifyComment(ClickHouseParser::AlterTableClauseModifyCommentContext *ctx)
{
    return AlterTableClause::createComment(!!ctx->IF(), visit(ctx->nestedIdentifier()), Literal::createString(ctx->STRING_LITERAL()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseModifyOrderBy(ClickHouseParser::AlterTableClauseModifyOrderByContext * ctx)
{
    return AlterTableClause::createOrderBy(visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseModifyRemove(ClickHouseParser::AlterTableClauseModifyRemoveContext *ctx)
{
    return AlterTableClause::createRemove(!!ctx->IF(), visit(ctx->nestedIdentifier()), visit(ctx->tableColumnPropertyType()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseModifyTTL(ClickHouseParser::AlterTableClauseModifyTTLContext *ctx)
{
    return AlterTableClause::createTTL(visit(ctx->ttlClause()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseRemoveTTL(ClickHouseParser::AlterTableClauseRemoveTTLContext *)
{
    return AlterTableClause::createRemoveTTL();
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseRename(ClickHouseParser::AlterTableClauseRenameContext *ctx)
{
    return AlterTableClause::createRename(!!ctx->IF(), visit(ctx->nestedIdentifier(0)), visit(ctx->nestedIdentifier(1)));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableClauseReplace(ClickHouseParser::AlterTableClauseReplaceContext *ctx)
{
    return AlterTableClause::createReplace(visit(ctx->partitionClause()), visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext * ctx)
{
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto list = std::make_shared<List<AlterTableClause>>();
    for (auto * clause : ctx->alterTableClause()) list->push(visit(clause));
    return std::make_shared<AlterTableQuery>(cluster, visit(ctx->tableIdentifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitTableColumnPropertyType(ClickHouseParser::TableColumnPropertyTypeContext *ctx)
{
    if (ctx->ALIAS()) return TableColumnPropertyType::ALIAS;
    if (ctx->CODEC()) return TableColumnPropertyType::CODEC;
    if (ctx->COMMENT()) return TableColumnPropertyType::COMMENT;
    if (ctx->DEFAULT()) return TableColumnPropertyType::DEFAULT;
    if (ctx->MATERIALIZED()) return TableColumnPropertyType::MATERIALIZED;
    if (ctx->TTL()) return TableColumnPropertyType::TTL;
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitPartitionClause(ClickHouseParser::PartitionClauseContext *ctx)
{
    if (ctx->STRING_LITERAL())
        return std::make_shared<PartitionClause>(Literal::createString(ctx->STRING_LITERAL()));

    auto expr = visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>();

    if (expr->getType() == ColumnExpr::ExprType::LITERAL)
        return std::make_shared<PartitionClause>(PtrTo<List<Literal>>(new List<Literal>{expr->getLiteral()}));

    if (expr->getType() == ColumnExpr::ExprType::FUNCTION && expr->getFunctionName() == "tuple")
    {
        auto list = std::make_shared<List<Literal>>();

        for (auto it = expr->argumentsBegin(); it != expr->argumentsEnd(); ++it)
        {
            auto * literal = (*it)->as<ColumnExpr>();

            if (literal->getType() == ColumnExpr::ExprType::LITERAL)
                list->push(literal->getLiteral());
            else
            {
                // TODO: 'Expected tuple of literals as Partition Expression'.
            }
        }

        return std::make_shared<PartitionClause>(list);
    }

    // TODO: 'Expected tuple of literals as Partition Expression'.
    __builtin_unreachable();
}

}
