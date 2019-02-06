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

    /// A hack to support hyphens in cluster names.
    if (const ASTFunction * ast_func = typeid_cast<const ASTFunction *>(&node))
    {
        if (ast_func->name != "minus" || !ast_func->arguments || ast_func->arguments->children.size() < 2)
            throw Exception("Illegal expression instead of cluster name.", ErrorCodes::BAD_ARGUMENTS);

        String name;
        for (const auto & arg : ast_func->arguments->children)
        {
            if (name.empty())
                name += getClusterName(*arg);
            else
                name += "-" + getClusterName(*arg);
        }

        return name;
    }

    throw Exception("Illegal expression instead of cluster name.", ErrorCodes::BAD_ARGUMENTS);
}

}
