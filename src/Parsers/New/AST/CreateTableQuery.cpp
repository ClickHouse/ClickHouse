#include <Parsers/New/AST/CreateTableQuery.h>

#include <IO/ReadHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableElementExpr.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<TableSchemaClause> TableSchemaClause::createDescription(PtrTo<TableElementList> list)
{
    return PtrTo<TableSchemaClause>(new TableSchemaClause(ClauseType::DESCRIPTION, {list}));
}

// static
PtrTo<TableSchemaClause> TableSchemaClause::createAsTable(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<TableSchemaClause>(new TableSchemaClause(ClauseType::TABLE, {identifier}));
}

// static
PtrTo<TableSchemaClause> TableSchemaClause::createAsFunction(PtrTo<TableFunctionExpr> expr)
{
    return PtrTo<TableSchemaClause>(new TableSchemaClause(ClauseType::FUNCTION, {expr}));
}

TableSchemaClause::TableSchemaClause(ClauseType type, PtrList exprs) : INode(exprs), clause_type(type)
{
}

ASTPtr TableSchemaClause::convertToOld() const
{
    switch(clause_type)
    {
        case ClauseType::DESCRIPTION:
        {
            auto columns = std::make_shared<ASTColumns>();

            auto column_list = std::make_shared<ASTExpressionList>();
            auto constraint_list = std::make_shared<ASTExpressionList>();
            auto index_list = std::make_shared<ASTExpressionList>();

            for (const auto & element : get(ELEMENTS)->as<TableElementList &>())
            {
                switch(element->as<TableElementExpr>()->getType())
                {
                    case TableElementExpr::ExprType::COLUMN:
                        column_list->children.push_back(element->convertToOld());
                        break;
                    case TableElementExpr::ExprType::CONSTRAINT:
                        constraint_list->children.push_back(element->convertToOld());
                        break;
                    case TableElementExpr::ExprType::INDEX:
                        index_list->children.push_back(element->convertToOld());
                        break;
                }
            }

            if (!column_list->children.empty()) columns->set(columns->columns, column_list);
            if (!constraint_list->children.empty()) columns->set(columns->constraints, constraint_list);
            if (!index_list->children.empty()) columns->set(columns->indices, index_list);

            return columns;
        }
        case ClauseType::FUNCTION:
        case ClauseType::TABLE:
            return get(EXPR)->convertToOld();
    }
    __builtin_unreachable();  // FIXME: old gcc compilers complain about reaching end of non-void function
}

String TableSchemaClause::dumpInfo() const
{
    switch(clause_type)
    {
        case ClauseType::DESCRIPTION: return "Description";
        case ClauseType::FUNCTION: return "Function";
        case ClauseType::TABLE: return "Table";
    }
    __builtin_unreachable();  // FIXME: old gcc compilers complain about reaching end of non-void function
}

CreateTableQuery::CreateTableQuery(
    PtrTo<ClusterClause> cluster,
    bool attach_,
    bool temporary_,
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<UUIDClause> uuid,
    PtrTo<TableSchemaClause> schema,
    PtrTo<EngineClause> engine,
    PtrTo<SelectUnionQuery> query)
    : DDLQuery(cluster, {identifier, uuid, schema, engine, query}), attach(attach_), temporary(temporary_), if_not_exists(if_not_exists_)
{
}

ASTPtr CreateTableQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    {
        auto table_id = getTableIdentifier(get(NAME)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid
            = has(UUID) ? parseFromString<DB::UUID>(get(UUID)->convertToOld()->as<ASTLiteral>()->value.get<String>()) : table_id.uuid;
    }

    query->cluster = cluster_name;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->temporary = temporary;

    if (has(SCHEMA))
    {
        switch(get<TableSchemaClause>(SCHEMA)->getType())
        {
            case TableSchemaClause::ClauseType::DESCRIPTION:
            {
                query->set(query->columns_list, get(SCHEMA)->convertToOld());
                break;
            }
            case TableSchemaClause::ClauseType::TABLE:
            {
                auto table_id = getTableIdentifier(get(SCHEMA)->convertToOld());
                query->as_database = table_id.database_name;
                query->as_table = table_id.table_name;
                break;
            }
            case TableSchemaClause::ClauseType::FUNCTION:
            {
                query->as_table_function = get(SCHEMA)->convertToOld();
                break;
            }
        }
    }
    if (has(ENGINE)) query->set(query->storage, get(ENGINE)->convertToOld());
    if (has(SUBQUERY)) query->set(query->select, get(SUBQUERY)->convertToOld());

    return query;
}

String CreateTableQuery::dumpInfo() const
{
    String info;
    if (attach) info += "attach=true, ";
    else info += "attach=false, ";
    if (temporary) info += "temporary=true, ";
    else info += "temporary=false, ";
    if (if_not_exists) info += "if_not_exists=true";
    else info += "if_not_exists=false";
    return info;
}

}

namespace DB
{

using namespace AST;

// TODO: assert(!(ctx->parent->TEMPORARY() ^ ctx->engineClause()))

antlrcpp::Any ParseTreeVisitor::visitClusterClause(ClickHouseParser::ClusterClauseContext *ctx)
{
    auto literal = ctx->STRING_LITERAL() ? Literal::createString(ctx->STRING_LITERAL())
                                         : Literal::createString(ctx->identifier()->getText());
    return std::make_shared<ClusterClause>(literal);
}

antlrcpp::Any ParseTreeVisitor::visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *ctx)
{
    auto uuid = ctx->uuidClause() ? visit(ctx->uuidClause()).as<PtrTo<UUIDClause>>() : nullptr;
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto schema = ctx->tableSchemaClause() ? visit(ctx->tableSchemaClause()).as<PtrTo<TableSchemaClause>>() : nullptr;
    auto engine = ctx->engineClause() ? visit(ctx->engineClause()).as<PtrTo<EngineClause>>() : nullptr;
    auto query = ctx->subqueryClause() ? visit(ctx->subqueryClause()).as<PtrTo<SelectUnionQuery>>() : nullptr;
    return std::make_shared<CreateTableQuery>(
        cluster, !!ctx->ATTACH(), !!ctx->TEMPORARY(), !!ctx->IF(), visit(ctx->tableIdentifier()), uuid, schema, engine, query);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *ctx)
{
    auto elems = std::make_shared<TableElementList>();
    for (auto * elem : ctx->tableElementExpr()) elems->push(visit(elem));
    return TableSchemaClause::createDescription(elems);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *ctx)
{
    return TableSchemaClause::createAsTable(visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *ctx)
{
    return TableSchemaClause::createAsFunction(visit(ctx->tableFunctionExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *ctx)
{
    return visit(ctx->selectUnionStmt());
}

antlrcpp::Any ParseTreeVisitor::visitUuidClause(ClickHouseParser::UuidClauseContext *ctx)
{
    return std::make_shared<UUIDClause>(Literal::createString(ctx->STRING_LITERAL()));
}

}
