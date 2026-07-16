#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const ASTUserNamesWithHost & names, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";

            const auto & user_name = name->as<const ASTUserNameWithHost &>();
            if (user_name.usernameWasQueryParameter())
                user_name.format(ostr, settings);
            else
                ostr << backQuoteIfNeed(user_name.toString());
        }
    }

    void formatRenameTo(const ASTUserNameWithHost & new_name, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " RENAME TO ";
        if (new_name.usernameWasQueryParameter())
            new_name.format(ostr, settings);
        else
            ostr << quoteString(new_name.toString());
    }

    void formatSettings(const ASTSettingsProfileElements & settings, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << " SETTINGS ";
        settings.format(ostr, format);
    }

    void formatAlterSettings(const ASTAlterSettingsProfileElements & alter_settings, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << " ";
        alter_settings.format(ostr, format);
    }
}


String ASTCreateRoleQuery::getID(char) const
{
    return "CreateRoleQuery";
}


ASTPtr ASTCreateRoleQuery::clone() const
{
    auto res = make_intrusive<ASTCreateRoleQuery>(*this);
    res->children.clear();

    if (names)
    {
        res->names = boost::static_pointer_cast<ASTUserNamesWithHost>(names->clone());
        if (res->names->hasQueryParameters())
            res->children.push_back(res->names);
    }

    if (new_name)
    {
        res->new_name = boost::static_pointer_cast<ASTUserNameWithHost>(new_name->clone());
        if (res->new_name->usernameWasQueryParameter())
            res->children.push_back(res->new_name);
    }

    if (settings)
        res->settings = boost::static_pointer_cast<ASTSettingsProfileElements>(settings->clone());

    if (alter_settings)
        res->alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(alter_settings->clone());

    return res;
}


void ASTCreateRoleQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        ostr << "ATTACH ROLE";
    }
    else
    {
        ostr << (alter ? "ALTER ROLE" : "CREATE ROLE")
                     ;
    }

    if (if_exists)
        ostr << " IF EXISTS";
    else if (if_not_exists)
        ostr << " IF NOT EXISTS";
    else if (or_replace)
        ostr << " OR REPLACE";

    ostr << " ";
    formatNames(*names, ostr, format);

    if (!storage_name.empty())
        ostr
                    << " IN "
                    << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, format);

    if (new_name)
        formatRenameTo(*new_name, ostr, format);

    if (alter_settings)
        formatAlterSettings(*alter_settings, ostr, format);
    else if (settings)
        formatSettings(*settings, ostr, format);
}

}
