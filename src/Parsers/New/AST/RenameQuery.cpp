#include <Parsers/New/AST/RenameQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

RenameQuery::RenameQuery(PtrTo<ClusterClause> cluster, PtrTo<List<TableIdentifier>> list) : DDLQuery(cluster, {list})
{
}

ASTPtr RenameQuery::convertToOld() const
{
    auto query = std::make_shared<ASTRenameQuery>();

    for (auto table = get<List<TableIdentifier>>(EXPRS)->begin(), end = get<List<TableIdentifier>>(EXPRS)->end(); table != end; ++table)
    {
        ASTRenameQuery::Element element;

        if (auto database = (*table)->as<TableIdentifier>()->getDatabase())
            element.from.database = database->getName();
        element.from.table = (*table)->as<TableIdentifier>()->getName();

        ++table;

        if (auto database = (*table)->as<TableIdentifier>()->getDatabase())
            element.to.database = database->getName();
        element.to.table = (*table)->as<TableIdentifier>()->getName();

        query->elements.push_back(element);
    }

    query->cluster = cluster_name;

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitRenameStmt(ClickHouseParser::RenameStmtContext *ctx)
{
    auto list = std::make_shared<List<TableIdentifier>>();
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    for (auto * identifier : ctx->tableIdentifier()) list->push(visit(identifier));
    return std::make_shared<RenameQuery>(cluster, list);
}

}
