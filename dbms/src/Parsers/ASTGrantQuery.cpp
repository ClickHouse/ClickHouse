#include <Parsers/ASTGrantQuery.h>
#include <common/StringRef.h>
#include <map>


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


const std::vector<std::pair<ASTGrantQuery::AccessType, String>> & ASTGrantQuery::getAccessTypeNames()
{
    static const std::vector<std::pair<ASTGrantQuery::AccessType, String>> result = []
    {
        return std::vector<std::pair<ASTGrantQuery::AccessType, String>>
        {
            {USAGE, "USAGE"},
            {SELECT, "SELECT"},
            {INSERT, "INSERT"},
            {DELETE, "DELETE"},
            {ALTER, "ALTER"},
            {CREATE, "CREATE"},
            {DROP, "DROP"},
            {ALL, "ALL"},
        };
    }();
    return result;
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
        /// Grant roles to roles.
        for (size_t i = 0; i != roles.size(); ++i)
            settings.ostr << (i != 0 ? ", " : " ") << backQuoteIfNeed(roles[i]);

        outputToRoles();

        if (with_grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH ADMIN OPTION" << (settings.hilite ? hilite_none : "");
        return;
    }

    /// Grant access to roles.
    size_t count = 0;
    if (access)
    {
        for (const auto & [access_type, access_name] : getAccessTypeNames())
        {
            if ((access & access_type) && (access_type != ALL))
                settings.ostr << (count++ ? ", " : " ")
                              << (settings.hilite ? hilite_keyword : "")
                              << access_name
                              << (settings.hilite ? hilite_none : "");
        }
    }

    if (!columns_access.empty())
    {
        std::map<StringRef, std::vector<String>> access_to_columns;
        for (const auto & [column_name, column_access] : columns_access)
        {
            auto x = column_access & ~access;
            if (x)
            {
                for (const auto & [access_type, access_name] : getAccessTypeNames())
                {
                    if ((x & access_type) && (access_type != ALL))
                        access_to_columns[access_name].emplace_back(column_name);
                    x &= ~access_type;
                    if (!x)
                        break;
                }
            }
        }

        for (auto & [column_access, column_names] : access_to_columns)
        {
            settings.ostr << (count++ ? ", " : " ")
                          << (settings.hilite ? hilite_keyword : "")
                          << column_access
                          << (settings.hilite ? hilite_none : "")
                          << "(";
            std::sort(column_names.begin(), column_names.end());
            for (size_t i = 0; i != column_names.size(); ++i)
                settings.ostr << (i != 0 ? ", " : "") << backQuoteIfNeed(column_names[i]);
            settings.ostr << ")";
        }
    }

    if (!count)
        settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "USAGE" << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON" << (settings.hilite ? hilite_none : "") << " ";
    if (!database.empty())
        settings.ostr << backQuoteIfNeed(database) + ".";
    else if (!use_current_database)
        settings.ostr << "*.";
    if (!table.empty())
        settings.ostr << backQuoteIfNeed(table);
    else
        settings.ostr << "*";

    outputToRoles();

    if (with_grant_option)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH GRANT OPTION" << (settings.hilite ? hilite_none : "");
}

}
