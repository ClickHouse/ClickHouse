#include <Parsers/New/AST/ShowQuery.h>

#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<ShowQuery> ShowQuery::createDictionaries(PtrTo<DatabaseIdentifier> from)
{
    return PtrTo<ShowQuery>(new ShowQuery(QueryType::DICTIONARIES, {from}));
}

ShowQuery::ShowQuery(QueryType type, PtrList exprs) : Query(exprs), query_type(type)
{
}

ASTPtr ShowQuery::convertToOld() const
{
    auto query = std::make_shared<ASTShowTablesQuery>();

    switch(query_type)
    {
        case QueryType::DICTIONARIES:
            query->dictionaries = true;
            if (has(FROM)) query->from = get<DatabaseIdentifier>(FROM)->getQualifiedName();
            break;
    }

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitShowDictionariesStmt(ClickHouseParser::ShowDictionariesStmtContext *ctx)
{
    auto from = ctx->databaseIdentifier() ? visit(ctx->databaseIdentifier()).as<PtrTo<DatabaseIdentifier>>() : nullptr;
    return ShowQuery::createDictionaries(from);
}

}
