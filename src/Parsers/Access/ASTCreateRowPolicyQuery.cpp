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
    void formatRenameTo(const String & new_short_name, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" RENAME TO ");
        out.ostr << backQuote(new_short_name);
    }


    void formatAsRestrictiveOrPermissive(bool is_restrictive, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" AS ");
        out.ostr << (is_restrictive ? "restrictive" : "permissive");
    }


    void formatFilterExpression(const ASTPtr & expr, const IAST::FormattingBuffer & out)
    {
        out.ostr << " ";
        if (expr)
            expr->format(out);
        else
            out.writeKeyword("NONE");
    }


    void formatForClause(const boost::container::flat_set<std::string_view> & commands, const String & filter, const String & check,
                         bool alter, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" FOR ");
        bool need_comma = false;
        for (const auto & command : commands)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.writeKeyword(command);
        }

        if (!filter.empty())
            out.writeKeyword(" USING");

        if (!check.empty() && (alter || (check != filter)))
            out.writeKeyword(" WITH CHECK");
    }


    void formatForClauses(const std::vector<std::pair<RowPolicyFilterType, ASTPtr>> & filters, bool alter,
                          const IAST::FormattingBuffer & out)
    {
        std::vector<std::pair<RowPolicyFilterType, String>> filters_as_strings;
        WriteBufferFromOwnString temp_buf;
        for (const auto & [filter_type, filter] : filters)
        {
            formatFilterExpression(filter, out.copyWithSettingsOnly(temp_buf));
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
                formatForClause(commands, filter, check, alter, out);
        }
        while (!filter.empty() || !check.empty());
    }


    void formatToRoles(const ASTRolesOrUsersSet & roles, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" TO ");
        roles.format(out);
    }
}


String ASTCreateRowPolicyQuery::getID(char) const
{
    return "CREATE ROW POLICY or ALTER ROW POLICY query";
}


ASTPtr ASTCreateRowPolicyQuery::clone() const
{
    auto res = std::make_shared<ASTCreateRowPolicyQuery>(*this);

    if (names)
        res->names = std::static_pointer_cast<ASTRowPolicyNames>(names->clone());

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    /// `res->filters` is already initialized by the copy constructor of ASTCreateRowPolicyQuery (see the first line of this function).
    /// But the copy constructor just copied the pointers inside `filters` instead of cloning.
    /// We need to make a deep copy and not a shallow copy, so we have to manually clone each pointer in `res->filters`.
    chassert(res->filters.size() == filters.size());
    for (auto & [_, res_filter] : res->filters)
    {
        if (res_filter)
            res_filter = res_filter->clone();
    }

    return res;
}


void ASTCreateRowPolicyQuery::formatImpl(const FormattingBuffer & out) const
{
    if (attach)
        out.writeKeyword("ATTACH ROW POLICY");
    else
        out.writeKeyword(alter ? "ALTER ROW POLICY" : "CREATE ROW POLICY");

    if (if_exists)
        out.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        out.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        out.writeKeyword(" OR REPLACE");

    out.ostr << " ";
    names->format(out);

    formatOnCluster(out);
    assert(names->cluster.empty());

    if (!new_short_name.empty())
        formatRenameTo(new_short_name, out.copy());

    if (is_restrictive)
        formatAsRestrictiveOrPermissive(*is_restrictive, out.copy());

    formatForClauses(filters, alter, out.copy());

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, out.copy());
}


void ASTCreateRowPolicyQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTag(current_user_name);
}

void ASTCreateRowPolicyQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (names)
        names->replaceEmptyDatabase(current_database);
}
}
