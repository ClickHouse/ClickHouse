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
            settings.writeKeyword("ID");
            settings.ostr << "(" << quoteString(str) << ")";
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
        settings.writeKeyword("NONE");
        return;
    }

    bool need_comma = false;

    if (all)
    {
        if (std::exchange(need_comma, true))
            settings.ostr << ", ";
        settings.writeKeyword(use_keyword_any ? "ANY" : "ALL");
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
            settings.writeKeyword("CURRENT_USER");
        }
    }

    if (except_current_user || !except_names.empty())
    {
        settings.writeKeyword(" EXCEPT ");
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
            settings.writeKeyword("CURRENT_USER");
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
