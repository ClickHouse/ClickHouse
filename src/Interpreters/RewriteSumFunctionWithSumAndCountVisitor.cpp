#include <Interpreters/RewriteSumFunctionWithSumAndCountVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <Poco/String.h>

namespace DB
{

void RewriteSumFunctionWithSumAndCountMatcher::visit(ASTPtr & ast, const Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
        visit(*func, ast, data);
}

/** Rewrites `sum(column +/- literal)` into two individual functions
 * `sum(column)` and `literal * count(column)`.
 * sum(column + literal) -> sum(column) + literal * count(column)
 * sum(literal + column) -> literal * count(column) + sum(column)
 * sum(column - literal) -> sum(column) - literal * count(column)
 * sum(literal - column) -> literal * count(column) - sum(column)
 */
void RewriteSumFunctionWithSumAndCountMatcher::visit(const ASTFunction & function, ASTPtr & ast, const Data & data)
{
    static const std::unordered_set<String> function_supported = {
        "plus",
        "minus"
    };

    if (!function.arguments || Poco::toLower(function.name) != "sum" || function.arguments->children.size() != 1)
        return;

    const auto * func_plus_minus = function.arguments->children[0]->as<ASTFunction>();

    if (!func_plus_minus || !function_supported.contains(Poco::toLower(func_plus_minus->name)) || func_plus_minus->arguments->children.size() != 2)
        return;

    size_t column_id;
    if (func_plus_minus->arguments->children[0]->as<ASTIdentifier>() && func_plus_minus->arguments->children[1]->as<ASTLiteral>())
        column_id = 0;
    else if (func_plus_minus->arguments->children[0]->as<ASTLiteral>() && func_plus_minus->arguments->children[1]->as<ASTIdentifier>())
        column_id = 1;
    else
        return;

    size_t literal_id = 1 - column_id;
    const auto * literal = func_plus_minus->arguments->children[literal_id]->as<ASTLiteral>();
    if (!literal)
        return;

    ///all the types listed are numbers and supported by 'plus' and 'minus'.
    Field::Types::Which literal_type = literal->value.getType();
    if (literal_type != Field::Types::UInt64 &&
        literal_type != Field::Types::Int64 &&
        literal_type != Field::Types::UInt128 &&
        literal_type != Field::Types::Int128 &&
        literal_type != Field::Types::UInt256 &&
        literal_type != Field::Types::Int256 &&
        literal_type != Field::Types::Float64 &&
        literal_type != Field::Types::Decimal32 &&
        literal_type != Field::Types::Decimal64 &&
        literal_type != Field::Types::Decimal128 &&
        literal_type != Field::Types::Decimal256)
        return;

    const auto * column = func_plus_minus->arguments->children[column_id]->as<ASTIdentifier>();
    if (!column)
        return;

    auto pos = IdentifierSemantic::getMembership(*column);
    if (!pos)
        pos = IdentifierSemantic::chooseTableColumnMatch(*column, data.tables, true);
    if (!pos)
        return;

    if (*pos >= data.tables.size())
        return;

    auto column_type_name = data.tables[*pos].columns.tryGetByName(column->shortName());
    if (!column_type_name)
        return;

    const auto column_type = column_type_name->type;
    if (!column_type || !isNumber(*column_type))
        return;

    const String & column_name = column_type_name->name;

    if (column_id == 0)
    {
        const auto new_ast = makeASTFunction(func_plus_minus->name,
                                                makeASTFunction("sum",
                                                                std::make_shared<ASTIdentifier>(column_name)
                                                                ),
                                                makeASTFunction("multiply",
                                                                std::make_shared<ASTLiteral>(* literal),
                                                                makeASTFunction("count", std::make_shared<ASTIdentifier>(column_name))
                                                                )
                                                );
        if (!new_ast)
            return;

        new_ast->setAlias(ast->tryGetAlias());
        ast = new_ast;
    }
    else if (column_id == 1)
    {
        const auto new_ast = makeASTFunction(func_plus_minus->name,
                                                makeASTFunction("multiply",
                                                                std::make_shared<ASTLiteral>(* literal),
                                                                makeASTFunction("count", std::make_shared<ASTIdentifier>(column_name))
                                                                ),
                                                makeASTFunction("sum",
                                                                std::make_shared<ASTIdentifier>(column_name)
                                                                )
                                                );
        if (!new_ast)
            return;

        new_ast->setAlias(ast->tryGetAlias());
        ast = new_ast;
    }
}

}
