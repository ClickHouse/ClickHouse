#include <Parsers/ParserShowIndexesQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

bool ParserShowIndexesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr from1;
    ASTPtr from2;

    String from1_str;
    String from2_str;

    auto query = std::make_shared<ASTShowIndexesQuery>();

    if (!ParserKeyword("SHOW").ignore(pos, expected))
        return false;

    if (ParserKeyword("EXTENDED").ignore(pos, expected))
        query->extended = true;

    if (!(ParserKeyword("INDEX").ignore(pos, expected) || ParserKeyword("INDEXES").ignore(pos, expected) || ParserKeyword("INDICES").ignore(pos, expected) || ParserKeyword("KEYS").ignore(pos, expected)))
        return false;

    if (ParserKeyword("FROM").ignore(pos, expected) || ParserKeyword("IN").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier().parse(pos, from1, expected))
            return false;
    }
    else
        return false;

    tryGetIdentifierNameInto(from1, from1_str);

    bool abbreviated_form = from1_str.contains("."); // FROM database.table
    if (abbreviated_form)
    {
        std::vector<String> split;
        boost::split(split, from1_str, boost::is_any_of("."));
        query->database = split[0];
        query->table = split[1];
    }
    else
    {
        if (ParserKeyword("FROM").ignore(pos, expected) || ParserKeyword("IN").ignore(pos, expected))
            if (!ParserIdentifier().parse(pos, from2, expected))
                return false;

        tryGetIdentifierNameInto(from2, from2_str);

        query->table = from1_str;
        query->database = from2_str;
    }

    if (ParserKeyword("WHERE").ignore(pos, expected))
        if (!ParserExpressionWithOptionalAlias(false).parse(pos, query->where_expression, expected))
            return false;

    node = query;

    return true;
}

}

