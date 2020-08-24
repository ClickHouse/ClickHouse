#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    void formatColumnNames(const Strings & columns, const IAST::FormatSettings & settings)
    {
        settings.ostr << "(";
        bool need_comma = false;
        for (const auto & column : columns)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << backQuoteIfNeed(column);
        }
        settings.ostr << ")";
    }


    void formatAccessRightsElements(const AccessRightsElements & elements, const IAST::FormatSettings & settings)
    {
        bool no_output = true;
        for (size_t i = 0; i != elements.size(); ++i)
        {
            const auto & element = elements[i];
            auto keywords = element.access_flags.toKeywords();
            if (keywords.empty() || (!element.any_column && element.columns.empty()))
                continue;

            for (const auto & keyword : keywords)
            {
                if (!std::exchange(no_output, false))
                    settings.ostr << ", ";

                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << keyword << (settings.hilite ? IAST::hilite_none : "");
                if (!element.any_column)
                    formatColumnNames(element.columns, settings);
            }

            bool next_element_on_same_db_and_table = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if ((element.database == next_element.database) && (element.any_database == next_element.any_database)
                    && (element.table == next_element.table) && (element.any_table == next_element.any_table))
                    next_element_on_same_db_and_table = true;
            }

            if (!next_element_on_same_db_and_table)
            {
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " ON " << (settings.hilite ? IAST::hilite_none : "");
                if (element.any_database)
                    settings.ostr << "*.";
                else if (!element.database.empty())
                    settings.ostr << backQuoteIfNeed(element.database) + ".";

                if (element.any_table)
                    settings.ostr << "*";
                else
                    settings.ostr << backQuoteIfNeed(element.table);
            }
        }

        if (no_output)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "USAGE ON " << (settings.hilite ? IAST::hilite_none : "") << "*.*";
    }


    void formatToRoles(const ASTRolesOrUsersSet & to_roles, ASTGrantQuery::Kind kind, const IAST::FormatSettings & settings)
    {
        using Kind = ASTGrantQuery::Kind;
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << ((kind == Kind::GRANT) ? " TO " : " FROM ")
                      << (settings.hilite ? IAST::hilite_none : "");
        to_roles.format(settings);
    }
}


String ASTGrantQuery::getID(char) const
{
    return "GrantQuery";
}


ASTPtr ASTGrantQuery::clone() const
{
    return std::make_shared<ASTGrantQuery>(*this);
}


void ASTGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (attach ? "ATTACH " : "") << ((kind == Kind::GRANT) ? "GRANT" : "REVOKE")
                  << (settings.hilite ? IAST::hilite_none : "");

    formatOnCluster(settings);

    if (kind == Kind::REVOKE)
    {
        if (grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " GRANT OPTION FOR" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " ADMIN OPTION FOR" << (settings.hilite ? hilite_none : "");
    }

    if (roles && !access_rights_elements.empty())
        throw Exception("Either roles or access rights elements should be set", ErrorCodes::LOGICAL_ERROR);

    settings.ostr << " ";
    if (roles)
        roles->format(settings);
    else
        formatAccessRightsElements(access_rights_elements, settings);

    formatToRoles(*to_roles, kind, settings);

    if (kind == Kind::GRANT)
    {
        if (grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH GRANT OPTION" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH ADMIN OPTION" << (settings.hilite ? hilite_none : "");
    }
}


void ASTGrantQuery::replaceEmptyDatabaseWithCurrent(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}


void ASTGrantQuery::replaceCurrentUserTagWithName(const String & current_user_name) const
{
    if (to_roles)
        to_roles->replaceCurrentUserTagWithName(current_user_name);
}
}
