#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << " RENAME TO " << backQuoteIfNeed(new_name);
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
    auto res = make_intrusive<ASTCreateMaskingPolicyQuery>(*this);

    if (roles)
        res->roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

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

    ostr << " ON ";
    if (!database.empty())
        ostr << backQuoteIfNeed(database) << ".";
    ostr << backQuoteIfNeed(table_name);

    formatOnCluster(ostr, settings);

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


void ASTCreateMaskingPolicyQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Only formatter-emitted state is folded — in particular
    /// `roles` is folded exactly when the formatter emits the `TO` clause — so the hash survives
    /// the format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(alter);
    hash_state.update(attach);
    hash_state.update(if_exists);
    hash_state.update(if_not_exists);
    hash_state.update(or_replace);

    hash_state.update(name);
    hash_state.update(database);
    hash_state.update(table_name);
    hash_state.update(cluster);
    hash_state.update(storage_name);
    hash_state.update(new_name);

    hash_state.update(static_cast<bool>(update_assignments));
    if (update_assignments)
        update_assignments->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(where_condition));
    if (where_condition)
        where_condition->updateTreeHash(hash_state, ignore_aliases);

    const bool emits_roles = roles && (!roles->empty() || alter);
    hash_state.update(emits_roles);
    if (emits_roles)
        roles->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(priority);
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
