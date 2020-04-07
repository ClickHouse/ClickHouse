#include <Parsers/ASTExtendedRoleSet.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    void formatRoleNameOrID(const String & str, bool is_id, const IAST::FormatSettings & settings)
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

void ASTExtendedRoleSet::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
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
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ALL" << (settings.hilite ? IAST::hilite_none : "");
    }
    else
    {
        for (auto & role : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            formatRoleNameOrID(role, id_mode, settings);
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

        for (auto & except_role : except_names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            formatRoleNameOrID(except_role, id_mode, settings);
        }

        if (except_current_user)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CURRENT_USER" << (settings.hilite ? IAST::hilite_none : "");
        }
    }
}
}
