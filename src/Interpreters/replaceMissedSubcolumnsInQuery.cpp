#include <Interpreters/replaceMissedSubcolumnsInQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

bool replaceMissedSubcolumnsInFunction(ASTPtr & ast, const String & column_name)
{
    bool is_replaced = false;

    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (column_name == identifier->getColumnName())
        {
            ast = std::make_shared<ASTLiteral>(Field());
            return true;
        }
    }
    else if (auto * node = ast->as<ASTFunction>())
    {
        if (node->arguments)
        {
            size_t num_arguments = node->arguments->children.size();
            for (size_t arg = 0; arg < num_arguments; ++arg)
            {
                auto & child = node->arguments->children[arg];
                if (replaceMissedSubcolumnsInFunction(child, column_name))
                    is_replaced = true;
            }
        }
    }
    else
    {
        for (auto & child : ast->children)
        {
            if (replaceMissedSubcolumnsInFunction(child, column_name))
                is_replaced = true;
        }
    }

    return is_replaced;
}

void replaceMissedSubcolumnsInQuery(ASTPtr & ast, const String & column_name)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        if (column_name == identifier->getColumnName())
        {
            auto literal = std::make_shared<ASTLiteral>(Field());
            literal->setAlias(identifier->getAliasOrColumnName());
            ast = literal;
        }
    }
    else if (auto * node = ast->as<ASTFunction>())
    {
        String function_alias = node->getAliasOrColumnName();
        if (replaceMissedSubcolumnsInFunction(ast, column_name))
            ast->setAlias(function_alias);
    }
    else
    {
        for (auto & child : ast->children)
            replaceMissedSubcolumnsInQuery(child, column_name);
    }
}

}
