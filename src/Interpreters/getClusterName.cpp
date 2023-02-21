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
    if (const auto * ast_id = node.as<ASTIdentifier>())
        return ast_id->name();

    if (const auto * ast_lit = node.as<ASTLiteral>())
        return ast_lit->value.safeGet<String>();

    /// A hack to support hyphens in cluster names.
    if (const auto * ast_func = node.as<ASTFunction>())
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


std::string getClusterNameAndMakeLiteral(ASTPtr & node)
{
    String cluster_name = getClusterName(*node);
    node = std::make_shared<ASTLiteral>(cluster_name);
    return cluster_name;
}

}
