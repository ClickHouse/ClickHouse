#include <Parsers/Access/ASTCreateRoleQuery.h>
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


void ASTCreateRoleQuery::formatImpl(FormattingBuffer out) const
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

    formatNames(names, out);
    formatOnCluster(out);

    if (!new_name.empty())
        formatRenameTo(new_name, out);

    if (settings && (!settings->empty() || alter))
        formatSettings(*settings, out);
}

}
