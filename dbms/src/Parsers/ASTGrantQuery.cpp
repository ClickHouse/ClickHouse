#include <Parsers/ASTGrantQuery.h>


namespace DB
{
String ASTGrantQuery::getID(char) const
{
    return (kind == Kind::GRANT) ? "GrantQuery" : "RevokeQuery";
}


ASTPtr ASTGrantQuery::clone() const
{
    return std::make_shared<ASTGrantQuery>(*this);
}


void ASTGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << ((kind == Kind::GRANT) ? "GRANT" : "REVOKE")
                  << (settings.hilite ? hilite_none : "");

    auto outputToRoles = [&]
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << ((kind == Kind::GRANT) ? " TO" : " FROM")
                      << (settings.hilite ? hilite_none : "");

        for (size_t i = 0; i != to_roles.size(); ++i)
            settings.ostr << (i != 0 ? ", " : " ") << backQuoteIfNeed(to_roles[i]);
    };

    if (!roles.empty())
    {
        /// Grant roles to roles
        for (size_t i = 0; i != roles.size(); ++i)
            settings.ostr << (i != 0 ? ", " : " ") << backQuoteIfNeed(roles[i]);

        outputToRoles();

        if (with_grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH ADMIN OPTION" << (settings.hilite ? hilite_none : "");
        return;
    }

    /// Grant access to roles
    size_t count = 0;
    auto outputAccess = [&](bool has_access, const char * access_name)
    {
        if (has_access)
        {
            settings.ostr << (count++ ? ", " : " ")
                          << (settings.hilite ? hilite_keyword : "")
                          << access_name
                          << (settings.hilite ? hilite_none : "");
        }
    };
    if (access)
    {
        outputAccess(access & AccessType::SELECT, "SELECT");
        outputAccess(access & AccessType::INSERT, "INSERT");
        outputAccess(access & AccessType::DELETE, "DELETE");
        outputAccess(access & AccessType::ALTER, "ALTER");
        outputAccess(access & AccessType::CREATE, "CREATE");
        outputAccess(access & AccessType::DROP, "DROP");
    }

    if (!columns.empty())
    {
        auto outputAccessWithColumns = [&](bool has_access, const char * access_name)
        {
            outputAccess(has_access, access_name);
            settings.ostr << "(";
            for (size_t i = 0; i != columns.size(); ++i)
                settings.ostr << (i != 0 ? ", " : " ") << backQuoteIfNeed(columns[i]);
            settings.ostr << ")";
        };
        outputAccessWithColumns(columns_access & AccessType::SELECT, "SELECT");
    }

    if (!count)
        outputAccess(true, "USAGE");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON" << (settings.hilite ? hilite_none : "");

    if (!database.empty())
        settings.ostr << " " << backQuoteIfNeed(database) + ".";
    else if (!use_current_database)
        settings.ostr << " *.";

    if (!table.empty())
        settings.ostr << backQuoteIfNeed(table);
    else
        settings.ostr << "*";

    outputToRoles();

    if (with_grant_option)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH GRANT OPTION" << (settings.hilite ? hilite_none : "");
}

}
