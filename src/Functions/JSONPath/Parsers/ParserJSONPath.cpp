#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPathQuery.h>

namespace DB
{
/**
 * Entry parser for JSONPath
 */
bool ParserJSONPath::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto ast_jsonpath = std::make_shared<ASTJSONPath>();
    ParserJSONPathQuery parser_jsonpath_query;

    /// Push back dot AST and brackets AST to query->children
    ASTPtr query;

    bool res = parser_jsonpath_query.parse(pos, query, expected);

    if (res)
    {
        /// Set ASTJSONPathQuery of ASTJSONPath
        ast_jsonpath->set(ast_jsonpath->jsonpath_query, query);
    }

    node = ast_jsonpath;
    return res;
}

}
