#include <Parsers/New/AST/CreateTableQuery.h>

#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableElementExpr.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Parsers/New/ParseTreeVisitor.h>
#include "Parsers/ASTIdentifier.h"


namespace DB::AST
{

// static
PtrTo<SchemaClause> SchemaClause::createDescription(PtrTo<TableElementList> list)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::DESCRIPTION, {list}));
}

// static
PtrTo<SchemaClause> SchemaClause::createAsTable(PtrTo<TableIdentifier> identifier)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::TABLE, {identifier}));
}

// static
PtrTo<SchemaClause> SchemaClause::createAsFunction(PtrTo<Identifier> identifier, PtrTo<TableArgList> list)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::FUNCTION, {identifier, list}));
}

SchemaClause::SchemaClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;
}

CreateTableQuery::CreateTableQuery(
    bool attach_,
    bool temporary_,
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<SchemaClause> schema,
    PtrTo<EngineClause> engine,
    PtrTo<SelectUnionQuery> query)
    : attach(attach_), temporary(temporary_), if_not_exists(if_not_exists_)
{
    children.push_back(identifier);
    children.push_back(schema);
    children.push_back(engine);
    children.push_back(query);
}

ASTPtr CreateTableQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();
    auto name = children[NAME]->convertToOld();

    query->database = name->as<ASTTableIdentifier>()->getDatabaseName();
    query->table = name->as<ASTTableIdentifier>()->getTableName();
    query->uuid = name->as<ASTTableIdentifier>()->uuid;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->temporary = temporary;

    if (has(SCHEMA))
    {
        switch(children[SCHEMA]->as<SchemaClause>()->getType())
        {
            case SchemaClause::ClauseType::DESCRIPTION:
            {
                query->set(query->columns_list, children[SCHEMA]->convertToOld());
                break;
            }
            case SchemaClause::ClauseType::TABLE:
            {
                auto as_table = children[SCHEMA]->convertToOld();
                query->as_database = as_table->as<ASTIdentifier>()->getDatabaseName();
                query->as_table = as_table->as<ASTIdentifier>()->getTableName();
                break;
            }
            case SchemaClause::ClauseType::FUNCTION:
            {
                query->as_table_function = children[SCHEMA]->convertToOld();
                break;
            }
        }
    }
    if (has(ENGINE)) query->set(query->storage, children[ENGINE]->convertToOld());
    if (has(SUBQUERY)) query->set(query->select, children[SUBQUERY]->convertToOld());

    return query;
}

}

namespace DB
{

using namespace AST;

// TODO: assert(!(ctx->parent->TEMPORARY() ^ ctx->engineClause()))

antlrcpp::Any ParseTreeVisitor::visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *ctx)
{
    auto elems = std::make_shared<TableElementList>();
    for (auto * elem : ctx->tableElementExpr()) elems->append(visit(elem).as<PtrTo<TableElementExpr>>());
    return SchemaClause::createDescription(elems);
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *ctx)
{
    return SchemaClause::createAsTable(visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *ctx)
{
    return SchemaClause::createAsFunction(
        visit(ctx->identifier()), ctx->tableArgList() ? visit(ctx->tableArgList()).as<PtrTo<TableArgList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *ctx)
{
    return visit(ctx->selectUnionStmt());
}

}
