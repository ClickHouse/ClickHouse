#include <Parsers/ASTRoleList.h>
#include <Common/quoteString.h>


namespace DB
{
void ASTRoleList::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }

    bool need_comma = false;
    if (current_user)
    {
        if (std::exchange(need_comma, true))
            settings.ostr << ", ";
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CURRENT_USER" << (settings.hilite ? IAST::hilite_none : "");
    }

    for (auto & role : roles)
    {
        if (std::exchange(need_comma, true))
            settings.ostr << ", ";
        settings.ostr << backQuoteIfNeed(role);
    }

    if (all_roles)
    {
        if (std::exchange(need_comma, true))
            settings.ostr << ", ";
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ALL" << (settings.hilite ? IAST::hilite_none : "");
        if (except_current_user || !except_roles.empty())
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " EXCEPT " << (settings.hilite ? IAST::hilite_none : "");
            need_comma = false;

            if (except_current_user)
            {
                if (std::exchange(need_comma, true))
                    settings.ostr << ", ";
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CURRENT_USER" << (settings.hilite ? IAST::hilite_none : "");
            }

            for (auto & except_role : except_roles)
            {
                if (std::exchange(need_comma, true))
                    settings.ostr << ", ";
                settings.ostr << backQuoteIfNeed(except_role);
            }
        }
    }
}
}
