#include "ParserXPath.h"

#include <Functions/XPath/ASTs/ASTXPath.h>
#include <Functions/XPath/Parsers/ParserXPathQuery.h>

namespace DB
{

bool ParserXPath::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto ast_xpath = std::make_shared<ASTXPath>();
    ParserXPathQuery parser_xpath_query;

    ASTPtr query;

    bool res = parser_xpath_query.parse(pos, query, expected);

    if (res)
    {
        ast_xpath->set(ast_xpath->xpath_query, query);
    }

    node = ast_xpath;
    return res;
}

}
