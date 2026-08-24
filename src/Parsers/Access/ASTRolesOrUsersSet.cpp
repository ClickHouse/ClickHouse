#include <Access/Common/AccessRightsElement.h>
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
