#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/formatAST.h>
#include <Common/quoteString.h>
#include <ext/range.h>
#include <boost/range/algorithm/transform.hpp>
#include <sstream>


namespace DB
{
namespace
{
    using ConditionType = RowPolicy::ConditionType;
    using ConditionTypeInfo = RowPolicy::ConditionTypeInfo;
    constexpr auto MAX_CONDITION_TYPE = RowPolicy::MAX_CONDITION_TYPE;


    void formatRenameTo(const String & new_short_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << backQuote(new_short_name);
    }


    void formatAsRestrictiveOrPermissive(bool is_restrictive, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS " << (is_restrictive ? "RESTRICTIVE" : "PERMISSIVE")
                      << (settings.hilite ? IAST::hilite_none : "");
    }


    void formatConditionalExpression(const ASTPtr & expr, const IAST::FormatSettings & settings)
    {
        if (expr)
            expr->format(settings);
        else
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NONE" << (settings.hilite ? IAST::hilite_none : "");
    }


    void formatCondition(const boost::container::flat_set<std::string_view> & commands, const String & filter, const String & check, bool alter, const IAST::FormatSettings & settings)
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
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " USING " << (settings.hilite ? IAST::hilite_none : "") << filter;

        if (!check.empty() && (alter || (check != filter)))
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH CHECK " << (settings.hilite ? IAST::hilite_none : "") << check;
    }


    void formatMultipleConditions(const std::array<std::optional<ASTPtr>, MAX_CONDITION_TYPE> & conditions, bool alter, const IAST::FormatSettings & settings)
    {
        std::array<String, MAX_CONDITION_TYPE> conditions_as_strings;
        std::stringstream temp_sstream;
        IAST::FormatSettings temp_settings(temp_sstream, settings);
        for (auto condition_type : ext::range(MAX_CONDITION_TYPE))
        {
            const auto & condition = conditions[condition_type];
            if (condition)
            {
                formatConditionalExpression(*condition, temp_settings);
                conditions_as_strings[condition_type] = temp_sstream.str();
                temp_sstream.str("");
            }
        }

        boost::container::flat_set<std::string_view> commands;
        String filter, check;

        do
        {
            commands.clear();
            filter.clear();
            check.clear();

            /// Collect commands using the same filter and check conditions.
            for (auto condition_type : ext::range(MAX_CONDITION_TYPE))
            {
                const String & condition = conditions_as_strings[condition_type];
                if (condition.empty())
                    continue;
                const auto & type_info = ConditionTypeInfo::get(condition_type);
                if (type_info.is_check)
                {
                    if (check.empty())
                        check = condition;
                    else if (check != condition)
                        continue;
                }
                else
                {
                    if (filter.empty())
                        filter = condition;
                    else if (filter != condition)
                        continue;
                }
                commands.emplace(type_info.command);
                conditions_as_strings[condition_type].clear(); /// Skip this condition on the next iteration.
            }

            if (!filter.empty() || !check.empty())
                formatCondition(commands, filter, check, alter, settings);
        }
        while (!filter.empty() || !check.empty());
    }


    void formatToRoles(const ASTRolesOrUsersSet & roles, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TO " << (settings.hilite ? IAST::hilite_none : "");
        roles.format(settings);
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

    if (is_restrictive)
        formatAsRestrictiveOrPermissive(*is_restrictive, settings);

    formatMultipleConditions(conditions, alter, settings);

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, settings);
}


void ASTCreateRowPolicyQuery::replaceCurrentUserTagWithName(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTagWithName(current_user_name);
}

void ASTCreateRowPolicyQuery::replaceEmptyDatabaseWithCurrent(const String & current_database) const
{
    if (names)
        names->replaceEmptyDatabaseWithCurrent(current_database);
}
}
