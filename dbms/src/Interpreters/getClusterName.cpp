#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>

#include <Interpreters/getClusterName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


std::string getClusterName(const IAST & node)
{
    if (const ASTIdentifier * ast_id = typeid_cast<const ASTIdentifier *>(&node))
        return ast_id->name;

    if (const ASTLiteral * ast_lit = typeid_cast<const ASTLiteral *>(&node))
        return ast_lit->value.safeGet<String>();

    if (const ASTFunction * ast_func = typeid_cast<const ASTFunction *>(&node))
    {
        if (!ast_func->range.first || !ast_func->range.second)
            throw Exception("Illegal expression instead of cluster name.", ErrorCodes::BAD_ARGUMENTS);

        return String(ast_func->range.first, ast_func->range.second);
    }

    throw Exception("Illegal expression instead of cluster name.", ErrorCodes::BAD_ARGUMENTS);
}

}
