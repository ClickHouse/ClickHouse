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
            formatNameOrID(name, id_mode, settings);
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
            formatNameOrID(name, id_mode, settings);
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

}
