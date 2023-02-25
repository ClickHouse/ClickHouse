#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNameOrID(const String & str, bool is_id, const IAST::FormatSettings & settings)
    {
        if (is_id)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ID" << (settings.hilite ? IAST::hilite_none : "") << "("
                          << quoteString(str) << ")";
        }
        else
        {
            settings.ostr << backQuoteIfNeed(str);
        }
    }
}

void ASTRolesOrUsersSet::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }

    bool need_comma = false;

    auto format_name = [&settings, this](const String & name, NameFilter filter)
    {
        if (filter == NameFilter::ANY || !enable_extended_subject_syntax)
        {
            formatNameOrID(name, id_mode, settings);
        }
        else
        {
            if (filter == NameFilter::USER)
            {
                formatNameOrID(name, id_mode, settings);
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS USER" << (settings.hilite ? IAST::hilite_none : "");
            }
            else if (filter == NameFilter::ROLE)
            {
                formatNameOrID(name, id_mode, settings);
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS ROLE" << (settings.hilite ? IAST::hilite_none : "");
            }
            else if (filter == NameFilter::BOTH)
            {
                formatNameOrID(name, id_mode, settings);
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS BOTH" << (settings.hilite ? IAST::hilite_none : "");
            }
        }
    };
    if (all)
    {
        if (std::exchange(need_comma, true))
            settings.ostr << ", ";
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (use_keyword_any ? "ANY" : "ALL")
                      << (settings.hilite ? IAST::hilite_none : "");
    }
    else
    {
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            format_name(name, getNameFilter(name));
        }

        if (current_user)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CURRENT_USER" << (settings.hilite ? IAST::hilite_none : "");
        }
    }

    if (except_current_user || !except_names.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " EXCEPT " << (settings.hilite ? IAST::hilite_none : "");
        need_comma = false;

        for (const auto & name : except_names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            format_name(name, getExceptNameFilter(name));
        }

        if (except_current_user)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CURRENT_USER" << (settings.hilite ? IAST::hilite_none : "");
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

ASTRolesOrUsersSet::NameFilter ASTRolesOrUsersSet::getNameFilter(const String & name) const
{
    return getFromFilters(name, names_filters);
}

ASTRolesOrUsersSet::NameFilter ASTRolesOrUsersSet::getExceptNameFilter(const String & name) const
{
    return getFromFilters(name, except_names_filters);
}

ASTRolesOrUsersSet::NameFilter ASTRolesOrUsersSet::getFromFilters(const String & name, const NameFilters & filters) const
{
    const auto n = filters.find(name);
    if (n != filters.end())
        return n->second;
    if (!allow_users && allow_roles)
        return ASTRolesOrUsersSet::NameFilter::ROLE;
    if (!allow_roles && allow_users)
        return ASTRolesOrUsersSet::NameFilter::USER;
    return {};
}
}
