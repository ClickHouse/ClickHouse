#include <Parsers/Access/ASTCreateRoleQuery.h>
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
            ostr << backQuoteIfNeed(name);
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
    auto res = std::make_shared<ASTCreateRoleQuery>(*this);

    if (settings)
        res->settings = std::static_pointer_cast<ASTSettingsProfileElements>(settings->clone());

    if (alter_settings)
        res->alter_settings = std::static_pointer_cast<ASTAlterSettingsProfileElements>(alter_settings->clone());

    return res;
}


void ASTCreateRoleQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        ostr << (format.hilite ? hilite_keyword : "") << "ATTACH ROLE" << (format.hilite ? hilite_none : "");
    }
    else
    {
        ostr << (format.hilite ? hilite_keyword : "") << (alter ? "ALTER ROLE" : "CREATE ROLE")
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

    if (alter_settings)
        formatAlterSettings(*alter_settings, ostr, format);
    else if (settings)
        formatSettings(*settings, ostr, format);
}

}
