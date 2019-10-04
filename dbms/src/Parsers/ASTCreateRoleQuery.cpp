#include <Parsers/ASTCreateRoleQuery.h>


namespace DB
{
String ASTCreateRoleQuery::getID(char) const
{
    return "CreateRoleQuery";
}


ASTPtr ASTCreateRoleQuery::clone() const
{
    return std::make_shared<ASTCreateRoleQuery>(*this);
}


void ASTCreateRoleQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "CREATE ROLE "
                  << (if_not_exists ? "IF NOT EXISTS " : "")
                  << (settings.hilite ? hilite_none : "");
    for (size_t i = 0; i != role_names.size(); ++i)
        settings.ostr << (i ? ", " : "") << backQuoteIfNeed(role_names[i]);
}
}
