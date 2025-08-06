#include "ParserXPathQuery.h"

#include <Functions/XPath/ASTs/ASTXPathQuery.h>

#include <Functions/XPath/Parsers/ParserXPathAttribute.h>
#include <Functions/XPath/Parsers/ParserXPathIndexAccess.h>
#include <Functions/XPath/Parsers/ParserXPathMemberAccess.h>
#include <Functions/XPath/Parsers/ParserXPathText.h>

namespace DB

{

bool ParserXPathQuery::parseImpl(Pos & pos, ASTPtr & query, Expected & expected)
{
    query = std::make_shared<ASTXPathQuery>();

    ParserXPathAttribute parser_xpath_attribute;
    ParserXPathIndexAccess parser_xpath_index_access;
    ParserXPathMemberAccess parser_xpath_member_access;
    ParserXPathText parser_xpath_text;

    ASTPtr root_member_access;
    if (!parser_xpath_member_access.parse(pos, root_member_access, expected))
    {
        return false;
    }
    query->children.push_back(root_member_access);

    ASTPtr accessor;
    while (parser_xpath_text.parse(pos, accessor, expected) || parser_xpath_attribute.parse(pos, accessor, expected)
           || parser_xpath_index_access.parse(pos, accessor, expected) || parser_xpath_member_access.parse(pos, accessor, expected))
    {
        if (accessor)
        {
            query->children.push_back(accessor);
            accessor = nullptr;
        }
    }

    /// parsing was successful if we reached the end of query by this point
    return pos->type == TokenType::EndOfStream;
}

}
