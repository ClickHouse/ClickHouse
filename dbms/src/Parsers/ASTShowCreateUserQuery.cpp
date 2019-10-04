#include <Parsers/ASTShowCreateUserQuery.h>


namespace DB
{
String ASTShowCreateUserQuery::getID(char) const
{
    return "ShowCreateUserQuery";
}


ASTPtr ASTShowCreateUserQuery::clone() const
{
    auto res = std::make_shared<ASTShowCreateUserQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}


void ASTShowCreateUserQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW CREATE USER"
                  << (settings.hilite ? hilite_none : "")
                  << " " + backQuoteIfNeed(user_name);
}
}
