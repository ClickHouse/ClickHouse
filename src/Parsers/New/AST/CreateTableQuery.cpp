#include <Parsers/New/AST/CreateTableQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableElementExpr.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


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
PtrTo<SchemaClause> SchemaClause::createAsFunction(PtrTo<TableFunctionExpr> expr)
{
    return PtrTo<SchemaClause>(new SchemaClause(ClauseType::FUNCTION, {expr}));
}

SchemaClause::SchemaClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;
}

ASTPtr SchemaClause::convertToOld() const
{
    switch(clause_type)
    {
        case ClauseType::DESCRIPTION:
        {
            auto columns = std::make_shared<ASTColumns>();
           // TODO
            return columns;
        }
        case ClauseType::FUNCTION:
        case ClauseType::TABLE:
            return children.front()->convertToOld();
    }
}

String SchemaClause::dumpInfo() const
{
    switch(clause_type)
    {
        case ClauseType::DESCRIPTION: return "Description";
        case ClauseType::FUNCTION: return "Function";
        case ClauseType::TABLE: return "Table";
    }
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
                query->set(query->as_table, children[SCHEMA]->convertToOld());
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

antlrcpp::Any ParseTreeVisitor::visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *ctx)
{
    auto schema = ctx->schemaClause() ? visit(ctx->schemaClause()).as<PtrTo<SchemaClause>>() : nullptr;
    auto engine = ctx->engineClause() ? visit(ctx->engineClause()).as<PtrTo<EngineClause>>() : nullptr;
    auto query = ctx->subqueryClause() ? visit(ctx->subqueryClause()).as<PtrTo<SelectUnionQuery>>() : nullptr;
    return std::make_shared<CreateTableQuery>(
        !!ctx->ATTACH(), !!ctx->TEMPORARY(), !!ctx->IF(), visit(ctx->tableIdentifier()), schema, engine, query);
}

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
    return SchemaClause::createAsFunction(visit(ctx->tableFunctionExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *ctx)
{
    return visit(ctx->selectUnionStmt());
}

}
