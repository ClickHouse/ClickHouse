#pragma once

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/** Replace predicate having Date/DateTime converters with their preimages to improve performance.
 *  Given a Date column c, toYear(c) = 2023 -> c >= '2023-01-01' AND c < '2024-01-01'
 *  Or if c is a DateTime column, toYear(c) = 2023 -> c >= '2023-01-01 00:00:00' AND c < '2024-01-01 00:00:00'.
 *  The similar optimization also applies to other converters.
 */
class OptimizeDateOrDateTimeConverterWithPreimageMatcher
{
public:
    struct Data
    {
        const TablesWithColumns & tables;
        ContextPtr context;
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        if (const auto * ast_function = ast->as<ASTFunction>())
            visit(*ast_function, ast, data);
    }

    static void visit(const ASTFunction & function, ASTPtr & ast, const Data & data);

    static bool needChildVisit(ASTPtr & ast, ASTPtr & child);
};

using OptimizeDateOrDateTimeConverterWithPreimageVisitor = InDepthNodeVisitor<OptimizeDateOrDateTimeConverterWithPreimageMatcher, true>;
}
