#include <Access/Common/AccessRightsElement.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <unordered_set>


namespace DB
{
namespace
{
    void formatNameOrID(const ASTUserNameWithHost & name, bool is_id, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        if (is_id)
            ostr << "ID(" << quoteString(name.toString()) << ")";
        else if (name.usernameWasQueryParameter())
            name.format(ostr, settings);
        else
            ostr << backQuoteIfNeed(name.toString());
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
        if (names)
        {
            for (const auto & name : *names)
            {
                if (std::exchange(need_comma, true))
                    ostr << ", ";
                formatNameOrID(name->as<const ASTUserNameWithHost &>(), id_mode, ostr, settings);
            }
        }

        if (current_user)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << "CURRENT_USER";
        }
    }

    if (except_current_user || (except_names && !except_names->children.empty()))
    {
        ostr << " EXCEPT ";
        need_comma = false;

        if (except_names)
        {
            for (const auto & name : *except_names)
            {
                if (std::exchange(need_comma, true))
                    ostr << ", ";
                formatNameOrID(name->as<const ASTUserNameWithHost &>(), id_mode, ostr, settings);
            }
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
        if (!names)
        {
            names = make_intrusive<ASTUserNamesWithHost>();
        }
        names->children.push_back(make_intrusive<ASTUserNameWithHost>(current_user_name));
        current_user = false;
    }

    if (except_current_user)
    {
        if (!except_names)
        {
            except_names = make_intrusive<ASTUserNamesWithHost>();
        }
        except_names->children.push_back(make_intrusive<ASTUserNameWithHost>(current_user_name));
        except_current_user = false;
    }
}


AccessRightsElements ASTRolesOrUsersSet::collectRequiredGrants(AccessType access_type) const
{
    AccessRightsElements res;
    const Strings except_all = except_names ? except_names->toStrings() : Strings{};
    std::unordered_set except(except_all.begin(), except_all.end());
    const Strings all_names = names ? names->toStrings() : Strings{};
    for (const auto & name : all_names)
    {
        if (except.contains(name))
            continue;

        res.push_back(AccessRightsElement(access_type, name));
    }

    if (all)
        res.push_back(AccessRightsElement(access_type));

    return res;
}


ASTPtr ASTRolesOrUsersSet::clone() const
{
    auto res = make_intrusive<ASTRolesOrUsersSet>(*this);

    res->children.clear();

    if (names)
    {
        res->names = boost::static_pointer_cast<ASTUserNamesWithHost>(names->clone());
        if (res->names->hasQueryParameters())
            res->children.push_back(res->names);
    }

    if (except_names)
    {
        res->except_names = boost::static_pointer_cast<ASTUserNamesWithHost>(except_names->clone());
        if (res->except_names->hasQueryParameters())
            res->children.push_back(res->except_names);
    }

    return res;
}


}
