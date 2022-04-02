#include <Parsers/Access/ASTShowAccessQuery.h>


namespace DB
{

String ASTShowAccessQuery::getID(char) const
{
    return "SHOW ACCESS Query";
}

ASTPtr ASTShowAccessQuery::clone() const
{
    return std::make_shared<ASTShowAccessQuery>(*this);
}

void ASTShowAccessQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW ACCESS" << (settings.hilite ? hilite_none : "");

    if (show_rbac_version)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH VERSION" << (settings.hilite ? hilite_none : "");
}

}
