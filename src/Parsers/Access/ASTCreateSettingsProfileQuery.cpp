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
        settings.writeKeyword(" RENAME TO ");
        settings.ostr << quoteString(new_name);
    }

    void formatSettings(const ASTSettingsProfileElements & settings, const IAST::FormatSettings & format)
    {
        format.writeKeyword(" SETTINGS ");
        settings.format(format);
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, const IAST::FormatSettings & settings)
    {
        settings.writeKeyword(" TO ");
        roles.format(settings);
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


void ASTCreateSettingsProfileQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        format.writeKeyword("ATTACH SETTINGS PROFILE");
    }
    else
    {
        format.writeKeyword(alter ? "ALTER SETTINGS PROFILE" : "CREATE SETTINGS PROFILE");
    }

    if (if_exists)
        format.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        format.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        format.writeKeyword(" OR REPLACE");

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
