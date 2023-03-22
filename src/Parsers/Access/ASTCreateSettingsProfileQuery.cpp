#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const Strings & names, IAST::FormattingBuffer out)
    {
        out.ostr << " ";
        bool need_comma = false;
        for (const String & name : names)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.ostr << backQuoteIfNeed(name);
        }
    }

    void formatRenameTo(const String & new_name, IAST::FormattingBuffer out)
    {
        out.writeKeyword(" RENAME TO ");
        out.ostr << quoteString(new_name);
    }

    void formatSettings(const ASTSettingsProfileElements & settings, IAST::FormattingBuffer out)
    {
        out.writeKeyword(" SETTINGS ");
        settings.formatImpl(out);
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, IAST::FormattingBuffer out)
    {
        out.writeKeyword(" TO ");
        roles.formatImpl(out);
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


void ASTCreateSettingsProfileQuery::formatImpl(FormattingBuffer out) const
{
    if (attach)
    {
        out.writeKeyword("ATTACH SETTINGS PROFILE");
    }
    else
    {
        out.writeKeyword(alter ? "ALTER SETTINGS PROFILE" : "CREATE SETTINGS PROFILE");
    }

    if (if_exists)
        out.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        out.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        out.writeKeyword(" OR REPLACE");

    formatNames(names, out);
    formatOnCluster(out);

    if (!new_name.empty())
        formatRenameTo(new_name, out);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, out);

    if (to_roles && (!to_roles->empty() || alter))
        formatToRoles(*to_roles, out);
}


void ASTCreateSettingsProfileQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (to_roles)
        to_roles->replaceCurrentUserTag(current_user_name);
}
}
