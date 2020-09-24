#include <Parsers/New/AST/OptimizeQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

OptimizeQuery::OptimizeQuery(PtrTo<TableIdentifier> identifier, PtrTo<PartitionExprList> list, bool final_, bool deduplicate_)
    : final(final_), deduplicate(deduplicate_)
{
    children.push_back(identifier);
    children.push_back(list);
}

ASTPtr OptimizeQuery::convertToOld() const
{
    auto query = std::make_shared<ASTOptimizeQuery>();

    {
        auto table_id = getTableIdentifier(children[TABLE]->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid = table_id.uuid;
    }

    if (has(PARTITION))
    {
        query->partition = children[PARTITION]->convertToOld();
        query->children.push_back(query->partition);
    }

    query->final = final;
    query->deduplicate = deduplicate;


    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *ctx)
{
    auto clause = ctx->partitionClause() ? visit(ctx->partitionClause()).as<PtrTo<PartitionExprList>>() : nullptr;
    return std::make_shared<OptimizeQuery>(visit(ctx->tableIdentifier()), clause, !!ctx->FINAL(), !!ctx->DEDUPLICATE());
}

antlrcpp::Any ParseTreeVisitor::visitPartitionClause(ClickHouseParser::PartitionClauseContext *ctx)
{
    auto list = std::make_shared<PartitionExprList>();

    if (ctx->STRING_LITERAL()) list->append(Literal::createString(ctx->STRING_LITERAL()));
    else
    {
        auto tuple = visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>();

        if (tuple->getType() == ColumnExpr::ExprType::FUNCTION && tuple->getFunctionName() == "tuple")
        {
            for (auto it = tuple->argumentsBegin(); it != tuple->argumentsEnd(); ++it)
            {
                auto * expr = (*it)->as<ColumnExpr>();

                if (expr->getType() == ColumnExpr::ExprType::LITERAL)
                    list->append(expr->getLiteral());
                else
                {
                    // TODO: 'Expected tuple of literals as Partition Expression'.
                }
            }
        }
        else
        {
            // TODO: 'Expected tuple of literals as Partition Expression'.
        }
    }

    return list;
}

}
