#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Common/quoteString.h>
#include <Common/SipHash.h>
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

    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << " RENAME TO " << quoteString(new_name);
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

    if (settings)
        res->settings = boost::static_pointer_cast<ASTSettingsProfileElements>(settings->clone());

    if (alter_settings)
        res->alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(alter_settings->clone());

    return res;
}


void ASTCreateRoleQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `getID` is constant and `children` is empty for this query, so every `CREATE`/`ALTER ROLE`
    /// collides in the base tree hash. The rewrite-rule matcher uses the tree hash for semantic
    /// equality, so fold every semantic field the formatter emits (and only those, so the hash
    /// survives the debug-build format -> parse -> format consistency check).
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);

    hash_state.update(alter);
    hash_state.update(attach);
    hash_state.update(if_exists);
    hash_state.update(if_not_exists);
    hash_state.update(or_replace);
    hash_state.update(cluster);
    hash_state.update(storage_name);
    hash_state.update(new_name);

    hash_state.update(names.size());
    for (const auto & name : names)
        hash_state.update(name);

    /// `settings` / `alter_settings` are `ASTSettingsProfileElements` members kept outside
    /// `children` (they already fold their own contents). The parser sets at most one of them.
    hash_state.update(static_cast<bool>(settings));
    if (settings)
        settings->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(alter_settings));
    if (alter_settings)
        alter_settings->updateTreeHash(hash_state, ignore_aliases);
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

    formatNames(names, ostr);

    if (!storage_name.empty())
        ostr
                    << " IN "
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
