#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const Strings & names, const IAST::FormatSettings & settings)
    {
        settings.ostr << " ";
        bool need_comma = false;
        for (const String & name : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << backQuoteIfNeed(name);
        }
    }

    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }

    void formatSettings(const ASTSettingsProfileElements & settings, const IAST::FormatSettings & format)
    {
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " SETTINGS " << (format.hilite ? IAST::hilite_none : "");
        settings.format(format);
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TO " << (settings.hilite ? IAST::hilite_none : "");
        roles.format(settings);
    }
}


String ASTCreateSettingsProfileQuery::getID(char) const
{
    return "CreateSettingsProfileQuery";
}


ASTPtr ASTCreateSettingsProfileQuery::clone() const
{
    return std::make_shared<ASTCreateSettingsProfileQuery>(*this);
}


void ASTCreateSettingsProfileQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << "ATTACH SETTINGS PROFILE" << (format.hilite ? hilite_none : "");
    }
    else
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (alter ? "ALTER SETTINGS PROFILE" : "CREATE SETTINGS PROFILE")
                      << (format.hilite ? hilite_none : "");
    }

    if (if_exists)
        format.ostr << (format.hilite ? hilite_keyword : "") << " IF EXISTS" << (format.hilite ? hilite_none : "");
    else if (if_not_exists)
        format.ostr << (format.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (format.hilite ? hilite_none : "");
    else if (or_replace)
        format.ostr << (format.hilite ? hilite_keyword : "") << " OR REPLACE" << (format.hilite ? hilite_none : "");

    formatNames(names, format);
    formatOnCluster(format);

    if (!new_name.empty())
        formatRenameTo(new_name, format);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, format);

    if (to_roles && (!to_roles->empty() || alter))
        formatToRoles(*to_roles, format);
}


void ASTCreateSettingsProfileQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (to_roles)
        to_roles->replaceCurrentUserTag(current_user_name);
}
}
