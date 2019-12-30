#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/formatAST.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/transform.hpp>
#include <sstream>


namespace DB
{
namespace
{
    using ConditionIndex = RowPolicy::ConditionIndex;

    void formatRenameTo(const String & new_policy_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << backQuote(new_policy_name);
    }


    void formatIsRestrictive(bool is_restrictive, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS " << (is_restrictive ? "RESTRICTIVE" : "PERMISSIVE")
                      << (settings.hilite ? IAST::hilite_none : "");
    }


    void formatConditionalExpression(const ASTPtr & expr, const IAST::FormatSettings & settings)
    {
        if (!expr)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NONE" << (settings.hilite ? IAST::hilite_none : "");
            return;
        }
        expr->format(settings);
    }


    std::vector<std::pair<ConditionIndex, String>>
    conditionalExpressionsToStrings(const std::vector<std::pair<ConditionIndex, ASTPtr>> & exprs, const IAST::FormatSettings & settings)
    {
        std::vector<std::pair<ConditionIndex, String>> result;
        std::stringstream ss;
        IAST::FormatSettings temp_settings(ss, settings);
        boost::range::transform(exprs, std::back_inserter(result), [&](const std::pair<ConditionIndex, ASTPtr> & in)
        {
            formatConditionalExpression(in.second, temp_settings);
            auto out = std::pair{in.first, ss.str()};
            ss.str("");
            return out;
        });
        return result;
    }


    void formatConditions(const char * op, const std::optional<String> & filter, const std::optional<String> & check, bool alter, const IAST::FormatSettings & settings)
    {
        if (op)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " FOR" << (settings.hilite ? IAST::hilite_none : "");
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << ' ' << op << (settings.hilite ? IAST::hilite_none : "");
        }

        if (filter)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " USING " << (settings.hilite ? IAST::hilite_none : "") << *filter;

        if (check && (alter || (check != filter)))
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WITH CHECK " << (settings.hilite ? IAST::hilite_none : "") << *check;
    }


    void formatMultipleConditions(const std::vector<std::pair<ConditionIndex, ASTPtr>> & conditions, bool alter, const IAST::FormatSettings & settings)
    {
        std::optional<String> scond[RowPolicy::MAX_CONDITION_INDEX];
        for (const auto & [index, scondition] : conditionalExpressionsToStrings(conditions, settings))
            scond[index] = scondition;

        if ((scond[RowPolicy::SELECT_FILTER] == scond[RowPolicy::UPDATE_FILTER])
            && (scond[RowPolicy::UPDATE_FILTER] == scond[RowPolicy::DELETE_FILTER])
            && (scond[RowPolicy::INSERT_CHECK] == scond[RowPolicy::UPDATE_CHECK])
            && (scond[RowPolicy::SELECT_FILTER] || scond[RowPolicy::INSERT_CHECK]))
        {
            formatConditions(nullptr, scond[RowPolicy::SELECT_FILTER], scond[RowPolicy::INSERT_CHECK], alter, settings);
            return;
        }

        bool need_comma = false;
        if (scond[RowPolicy::SELECT_FILTER])
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            formatConditions("SELECT", scond[RowPolicy::SELECT_FILTER], {}, alter, settings);
        }
        if (scond[RowPolicy::INSERT_CHECK])
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            formatConditions("INSERT", {}, scond[RowPolicy::INSERT_CHECK], alter, settings);
        }
        if (scond[RowPolicy::UPDATE_FILTER] || scond[RowPolicy::UPDATE_CHECK])
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            formatConditions("UPDATE", scond[RowPolicy::UPDATE_FILTER], scond[RowPolicy::UPDATE_CHECK], alter, settings);
        }
        if (scond[RowPolicy::DELETE_FILTER])
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            formatConditions("DELETE", scond[RowPolicy::DELETE_FILTER], {}, alter, settings);
        }
    }

    void formatRoles(const ASTRoleList & roles, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TO " << (settings.hilite ? IAST::hilite_none : "");
        roles.format(settings);
    }
}


String ASTCreateRowPolicyQuery::getID(char) const
{
    return "CREATE POLICY or ALTER POLICY query";
}


ASTPtr ASTCreateRowPolicyQuery::clone() const
{
    return std::make_shared<ASTCreateRowPolicyQuery>(*this);
}


void ASTCreateRowPolicyQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << (alter ? "ALTER POLICY" : "CREATE POLICY")
                  << (settings.hilite ? hilite_none : "");

    if (if_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF EXISTS" << (settings.hilite ? hilite_none : "");
    else if (if_not_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " OR REPLACE" << (settings.hilite ? hilite_none : "");

    const String & database = name_parts.database;
    const String & table_name = name_parts.table_name;
    const String & policy_name = name_parts.policy_name;
    settings.ostr << " " << backQuoteIfNeed(policy_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                  << (settings.hilite ? hilite_none : "") << (database.empty() ? String{} : backQuoteIfNeed(database) + ".") << table_name;

    if (!new_policy_name.empty())
        formatRenameTo(new_policy_name, settings);

    if (is_restrictive)
        formatIsRestrictive(*is_restrictive, settings);

    formatMultipleConditions(conditions, alter, settings);

    if (roles)
        formatRoles(*roles, settings);
}
}
