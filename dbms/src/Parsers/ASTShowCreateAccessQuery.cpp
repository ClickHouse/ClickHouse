#include <Parsers/ASTShowCreateAccessQuery.h>


namespace DB
{
String ASTShowCreateAccessQuery::getID(char) const
{
    return "ShowCreateAccessQuery";
}


ASTPtr ASTShowCreateAccessQuery::clone() const
{
    auto res = std::make_shared<ASTShowCreateAccessQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}


void ASTShowCreateAccessQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW CREATE "
                  << ((kind == Kind::USER) ? "USER" : "SETTINGS PROFILE")
                  << (settings.hilite ? hilite_none : "")
                  << " " + backQuoteIfNeed(name);
}
}
