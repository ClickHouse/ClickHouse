#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/formatAST.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <base/range.h>
#include <boost/container/flat_set.hpp>
#include <boost/range/algorithm/transform.hpp>


namespace DB
{
namespace
{
    void formatRenameTo(const String & new_short_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << backQuote(new_short_name);
    }


    void formatAsKind(RowPolicyKind kind, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS " << (settings.hilite ? IAST::hilite_none : "")
                      << RowPolicyKindInfo::get(kind).name;
    }


    void formatFilterExpression(const ASTPtr & expr, const IAST::FormatSettings & settings)
    {
        settings.ostr << " ";
        if (expr)
            expr->format(settings);
        else
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
    }


    void formatForClause(const boost::container::flat_set<std::string_view> & commands, const String & filter, const String & check, bool alter, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " FOR " << (settings.hilite ? IAST::hilite_none : "");
        bool need_comma = false;
        for (const auto & command : commands)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << command << (settings.hilite ? IAST::hilite_none : "");
        }

        if (!filter.empty())
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " USING" << (settings.hilite ? IAST::hilite_none : "") << filter;

        if (!check.empty() && (alter || (check != filter)))
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH CHECK" << (settings.hilite ? IAST::hilite_none : "") << check;
    }


    void formatForClauses(const std::vector<std::pair<RowPolicyFilterType, ASTPtr>> & filters, bool alter, const IAST::FormatSettings & settings)
    {
        std::vector<std::pair<RowPolicyFilterType, String>> filters_as_strings;
        WriteBufferFromOwnString temp_buf;
        IAST::FormatSettings temp_settings(temp_buf, settings);
        for (const auto & [filter_type, filter] : filters)
        {
            formatFilterExpression(filter, temp_settings);
            filters_as_strings.emplace_back(filter_type, temp_buf.str());
            temp_buf.restart();
        }

        boost::container::flat_set<std::string_view> commands;
        String filter, check;

        do
        {
            commands.clear();
            filter.clear();
            check.clear();

            /// Collect commands using the same filter and check conditions.
            for (auto & [filter_type, str] : filters_as_strings)
            {
                if (str.empty())
                    continue;
                const auto & type_info = RowPolicyFilterTypeInfo::get(filter_type);
                if (type_info.is_check)
                {
                    if (check.empty())
                        check = str;
                    else if (check != str)
                        continue;
                }
                else
                {
                    if (filter.empty())
                        filter = str;
                    else if (filter != str)
                        continue;
                }
                commands.emplace(type_info.command);
                str.clear(); /// Skip this condition on the next iteration.
            }

            if (!filter.empty() || !check.empty())
                formatForClause(commands, filter, check, alter, settings);
        }
        while (!filter.empty() || !check.empty());
    }


    void formatToSet(const ASTRolesOrUsersSet & to_set, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TO " << (settings.hilite ? IAST::hilite_none : "");
        to_set.format(settings);
    }

    void formatOfSet(const ASTRolesOrUsersSet & of_set, bool same_as_to_set, const IAST::FormatSettings & settings)
    {
        if (same_as_to_set)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " ONLY" << (settings.hilite ? IAST::hilite_none : "");
        }
        else
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " OF " << (settings.hilite ? IAST::hilite_none : "");
            of_set.format(settings);
        }
    }
}


String ASTCreateRowPolicyQuery::getID(char) const
{
    return "CREATE ROW POLICY or ALTER ROW POLICY query";
}


ASTPtr ASTCreateRowPolicyQuery::clone() const
{
    return std::make_shared<ASTCreateRowPolicyQuery>(*this);
}


void ASTCreateRowPolicyQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ATTACH ROW POLICY";
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (alter ? "ALTER ROW POLICY" : "CREATE ROW POLICY")
                      << (settings.hilite ? hilite_none : "");
    }

    if (if_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF EXISTS" << (settings.hilite ? hilite_none : "");
    else if (if_not_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " OR REPLACE" << (settings.hilite ? hilite_none : "");

    settings.ostr << " ";
    names->format(settings);

    formatOnCluster(settings);
    assert(names->cluster.empty());

    if (!new_short_name.empty())
        formatRenameTo(new_short_name, settings);

    if (kind)
        formatAsKind(*kind, settings);

    formatForClauses(filters, alter, settings);

    if (to_set || of_set)
    {
        auto to_set_not_null = to_set ? to_set : std::make_shared<ASTRolesOrUsersSet>();
        formatToSet(*to_set_not_null, settings);
        if (of_set)
            formatOfSet(*of_set, of_set->equals(*to_set_not_null), settings);
    }
}


void ASTCreateRowPolicyQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (to_set)
        to_set->replaceCurrentUserTag(current_user_name);
    if (of_set)
        of_set->replaceCurrentUserTag(current_user_name);
}

void ASTCreateRowPolicyQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (names)
        names->replaceEmptyDatabase(current_database);
}
}
