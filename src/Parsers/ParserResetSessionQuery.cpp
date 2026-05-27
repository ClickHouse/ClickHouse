#include <Parsers/ParserResetSessionQuery.h>

#include <Parsers/ASTResetSessionQuery.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool ParserResetSessionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_reset_session(Keyword::RESET_SESSION);

    if (!s_reset_session.ignore(pos, expected))
        return false;

    node = make_intrusive<ASTResetSessionQuery>();
    return true;
}

}
