#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const Strings & names, WriteBuffer & ostr)
    {
        ostr << " ";
        bool need_comma = false;
        for (const String & name : names)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << backQuote(name);
        }
    }

    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }

    void formatSettings(const ASTSettingsProfileElements & settings, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << (format.hilite ? IAST::hilite_keyword : "") << " SETTINGS " << (format.hilite ? IAST::hilite_none : "");
        settings.format(ostr, format);
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TO " << (settings.hilite ? IAST::hilite_none : "");
        roles.format(ostr, settings);
    }
}


String ASTCreateSettingsProfileQuery::getID(char) const
{
    return "CreateSettingsProfileQuery";
}


ASTPtr ASTCreateSettingsProfileQuery::clone() const
{
    auto res = std::make_shared<ASTCreateSettingsProfileQuery>(*this);

    if (to_roles)
        res->to_roles = std::static_pointer_cast<ASTRolesOrUsersSet>(to_roles->clone());

    if (settings)
        res->settings = std::static_pointer_cast<ASTSettingsProfileElements>(settings->clone());

    return res;
}


void ASTCreateSettingsProfileQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        ostr << (format.hilite ? hilite_keyword : "") << "ATTACH SETTINGS PROFILE" << (format.hilite ? hilite_none : "");
    }
    else
    {
        ostr << (format.hilite ? hilite_keyword : "") << (alter ? "ALTER SETTINGS PROFILE" : "CREATE SETTINGS PROFILE")
                      << (format.hilite ? hilite_none : "");
    }

    if (if_exists)
        ostr << (format.hilite ? hilite_keyword : "") << " IF EXISTS" << (format.hilite ? hilite_none : "");
    else if (if_not_exists)
        ostr << (format.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (format.hilite ? hilite_none : "");
    else if (or_replace)
        ostr << (format.hilite ? hilite_keyword : "") << " OR REPLACE" << (format.hilite ? hilite_none : "");

    formatNames(names, ostr);

    if (!storage_name.empty())
        ostr << (format.hilite ? IAST::hilite_keyword : "")
                    << " IN " << (format.hilite ? IAST::hilite_none : "")
                    << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, format);

    if (!new_name.empty())
        formatRenameTo(new_name, ostr, format);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, ostr, format);

    if (to_roles && (!to_roles->empty() || alter))
        formatToRoles(*to_roles, ostr, format);
}


void ASTCreateSettingsProfileQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (to_roles)
        to_roles->replaceCurrentUserTag(current_user_name);
}
}
