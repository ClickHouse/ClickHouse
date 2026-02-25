#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << " RENAME TO " << backQuote(new_name);
    }

    void formatUpdateExpression(const ASTPtr & expr, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " UPDATE ";
        expr->format(ostr, settings);
    }

    void formatWhereCondition(const ASTPtr & condition, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " WHERE ";
        condition->format(ostr, settings);
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " TO ";
        roles.format(ostr, settings);
    }

    void formatPriority(Int64 priority, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << " PRIORITY " << priority;
    }
}


String ASTCreateMaskingPolicyQuery::getID(char) const
{
    return "CREATE MASKING POLICY or ALTER MASKING POLICY query";
}


ASTPtr ASTCreateMaskingPolicyQuery::clone() const
{
    auto res = std::make_shared<ASTCreateMaskingPolicyQuery>(*this);

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (update_assignments)
        res->update_assignments = update_assignments->clone();

    if (where_condition)
        res->where_condition = where_condition->clone();

    return res;
}


void ASTCreateMaskingPolicyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        ostr << "ATTACH MASKING POLICY";
    }
    else
    {
        ostr << (alter ? "ALTER MASKING POLICY" : "CREATE MASKING POLICY");
    }

    if (if_exists)
        ostr << " IF EXISTS";
    else if (if_not_exists)
        ostr << " IF NOT EXISTS";
    else if (or_replace)
        ostr << " OR REPLACE";

    ostr << " " << backQuoteIfNeed(name);

    formatOnCluster(ostr, settings);

    ostr << " ON ";
    if (!database.empty())
        ostr << backQuoteIfNeed(database) << ".";
    ostr << backQuoteIfNeed(table_name);

    if (!storage_name.empty())
        ostr << " IN " << backQuoteIfNeed(storage_name);

    if (!new_name.empty())
        formatRenameTo(new_name, ostr, settings);

    if (update_assignments)
        formatUpdateExpression(update_assignments, ostr, settings);

    if (where_condition)
        formatWhereCondition(where_condition, ostr, settings);

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, ostr, settings);

    if (priority != 0)
        formatPriority(priority, ostr, settings);
}


void ASTCreateMaskingPolicyQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTag(current_user_name);
}

void ASTCreateMaskingPolicyQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (database.empty())
        const_cast<ASTCreateMaskingPolicyQuery *>(this)->database = current_database;
}
}
