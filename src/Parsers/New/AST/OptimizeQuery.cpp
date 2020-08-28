#include <Parsers/New/AST/OptimizeQuery.h>

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
    if (list) children.insert(children.end(), list->begin(), list->end());
    (void)final, (void)deduplicate; // TODO
}

}

namespace DB
{

using namespace AST;

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
