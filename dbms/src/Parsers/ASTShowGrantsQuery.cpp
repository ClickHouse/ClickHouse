#include <Parsers/ASTShowGrantsQuery.h>


namespace DB
{
String ASTShowGrantsQuery::getID(char) const
{
    return "ShowGrantsQuery";
}


ASTPtr ASTShowGrantsQuery::clone() const
{
    auto res = std::make_shared<ASTShowGrantsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}


void ASTShowGrantsQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW GRANTS FOR"
                  << (settings.hilite ? hilite_none : "")
                  << " " + backQuoteIfNeed(role);
}
}
