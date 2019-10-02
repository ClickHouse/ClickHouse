#include <Parsers/ASTDropACQuery.h>


namespace DB
{
ASTPtr ASTDropRoleQuery::clone() const
{
    return std::make_shared<ASTDropRoleQuery>(*this);
}


void ASTDropRoleQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP ROLE "
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    for (size_t i = 0; i != role_names.size(); ++i)
    {
        settings.ostr << (i ? ", " : " ");
        role_names[i]->format(settings);
    }
}


ASTPtr ASTDropUserQuery::clone() const
{
    return std::make_shared<ASTDropUserQuery>(*this);
}


void ASTDropUserQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP USER "
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    for (size_t i = 0; i != user_names.size(); ++i)
    {
        settings.ostr << (i ? ", " : " ");
        user_names[i]->format(settings);
    }
}
}
