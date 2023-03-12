#include <Parsers/Access/ASTCreateRoleQuery.h>
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

    return res;
}


void ASTCreateRoleQuery::formatImpl(const FormattingBuffer & out) const
{
    if (attach)
    {
        out.writeKeyword("ATTACH ROLE");
    }
    else
    {
        out.writeKeyword(alter ? "ALTER ROLE" : "CREATE ROLE");
    }

    if (if_exists)
        out.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        out.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        out.writeKeyword(" OR REPLACE");

    formatNames(names, out.copy());
    formatOnCluster(out);

    if (!new_name.empty())
        formatRenameTo(new_name, out.copy());

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, out.copy());
}

}
