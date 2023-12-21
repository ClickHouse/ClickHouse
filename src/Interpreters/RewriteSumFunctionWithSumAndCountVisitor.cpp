#include <Interpreters/RewriteSumFunctionWithSumAndCountVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

void RewriteSumFunctionWithSumAndCountMatcher::visit(ASTPtr & ast, const Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
        visit(*func, ast, data);
}

/** Rewrite the following AST to break the function `sum(column + literal)` into two individual functions
 * `sum(column)` and `literal * count(column)`.
 *  sum(column + literal)  ->  sum(column) + literal * count(column)
 *  sum(literal + column)  ->  sum(column) + literal * count(column)
 */
void RewriteSumFunctionWithSumAndCountMatcher::visit(const ASTFunction & function, ASTPtr & ast, const Data & data)
{
    static const std::unordered_set<String> nested_func_supported = {
        "plus",
        "minus"
    };

    if (!function.arguments || Poco::toLower(function.name) != "sum" || function.arguments->children.size() != 1)
        return;

    const auto * nested_func = function.arguments->children[0]->as<ASTFunction>();

    if (!nested_func || !nested_func_supported.contains(Poco::toLower(nested_func->name))|| nested_func->arguments->children.size() != 2)
        return;

    String alias = nested_func->tryGetAlias();
    if (!alias.empty())
        return;

    size_t column_id = nested_func->arguments->children.size();

    for (size_t i = 0; i < nested_func->arguments->children.size(); i++)
        if (nested_func->arguments->children[i]->as<ASTIdentifier>())
            column_id = i;

    if (column_id == nested_func->arguments->children.size())
        return;

    size_t literal_id = 1 - column_id;
    const auto * literal = nested_func->arguments->children[literal_id]->as<ASTLiteral>();
    if (!literal)
        return;

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

    const auto * column = nested_func->arguments->children[column_id]->as<ASTIdentifier>();
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

    const auto new_ast = makeASTFunction(nested_func->name,
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

    ast = new_ast;

}

}
