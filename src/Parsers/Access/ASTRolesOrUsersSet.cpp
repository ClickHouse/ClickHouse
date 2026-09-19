#include <Access/Common/AccessRightsElement.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <unordered_set>


namespace DB
{
namespace
{
    void formatNameOrID(const String & str, bool is_id, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        if (is_id)
            ostr << "ID(" << quoteString(str) << ")";
        else
            ostr << backQuoteIfNeed(str);
    }
}

void ASTRolesOrUsersSet::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment. Each field is
    /// produced by the formatter, so it survives the format -> parse round-trip that the debug-build
    /// AST consistency check requires.
    hash_state.update(all);
    hash_state.update(current_user);
    hash_state.update(except_current_user);
    hash_state.update(names.size());
    for (const auto & name : names)
        hash_state.update(name);
    hash_state.update(except_names.size());
    for (const auto & name : except_names)
        hash_state.update(name);
}

void ASTRolesOrUsersSet::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (empty())
    {
        ostr << "NONE";
        return;
    }

    bool need_comma = false;

    if (all)
    {
        if (std::exchange(need_comma, true))
            ostr << ", ";
        ostr << (use_keyword_any ? "ANY" : "ALL")
                     ;
    }
    else
    {
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            formatNameOrID(name, id_mode, ostr, settings);
        }

        if (current_user)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << "CURRENT_USER";
        }
    }

    if (except_current_user || !except_names.empty())
    {
        ostr << " EXCEPT ";
        need_comma = false;

        for (const auto & name : except_names)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            formatNameOrID(name, id_mode, ostr, settings);
        }

        if (except_current_user)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << "CURRENT_USER";
        }
    }
}


void ASTRolesOrUsersSet::replaceCurrentUserTag(const String & current_user_name)
{
    if (current_user)
    {
        names.push_back(current_user_name);
        current_user = false;
    }

    if (except_current_user)
    {
        except_names.push_back(current_user_name);
        except_current_user = false;
    }
}

AccessRightsElements ASTRolesOrUsersSet::collectRequiredGrants(AccessType access_type)
{
    AccessRightsElements res;
    std::unordered_set<String> except(except_names.begin(), except_names.end());
    for (const auto & name: names)
    {
        if (except.contains(name))
            continue;

        res.push_back(AccessRightsElement(access_type, name));
    }

    if (all)
        res.push_back(AccessRightsElement(access_type));

    return res;
}


}
