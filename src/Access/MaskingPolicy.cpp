#include <Access/MaskingPolicy.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


String MaskingPolicyName::toString() const
{
    return backQuoteIfNeed(short_name) + " ON " + (database.empty() ? String() : backQuoteIfNeed(database) + ".") + backQuoteIfNeed(table_name);
}


void MaskingPolicy::setDatabase(const String & database)
{
    full_name.database = database;
    IAccessEntity::setName(full_name.toString());
}

void MaskingPolicy::setTableName(const String & table_name)
{
    full_name.table_name = table_name;
    IAccessEntity::setName(full_name.toString());
}

void MaskingPolicy::setShortName(const String & short_name)
{
    full_name.short_name = short_name;
    IAccessEntity::setName(full_name.toString());
}

void MaskingPolicy::setFullName(const String & short_name, const String & database, const String & table_name)
{
    full_name.short_name = short_name;
    full_name.database = database;
    full_name.table_name = table_name;
    IAccessEntity::setName(full_name.toString());
}

void MaskingPolicy::setFullName(const MaskingPolicyName & full_name_)
{
    full_name = full_name_;
    IAccessEntity::setName(full_name.toString());
}

void MaskingPolicy::setName(const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaskingPolicy::setName is not implemented");
}


bool MaskingPolicy::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;

    const auto & other_policy = typeid_cast<const MaskingPolicy &>(other);

    IAST::FormatSettings format_settings(/*one_line=*/true);
    WriteBufferFromOwnString update_assignments_formated;
    WriteBufferFromOwnString other_update_assignments_formated;
    WriteBufferFromOwnString where_condition_formated;
    WriteBufferFromOwnString other_where_condition_formated;

    if (update_assignments)
        update_assignments->format(update_assignments_formated, format_settings);

    if (other_policy.update_assignments)
        other_policy.update_assignments->format(other_update_assignments_formated, format_settings);

    if (where_condition)
        where_condition->format(where_condition_formated, format_settings);

    if (other_policy.where_condition)
        other_policy.where_condition->format(other_where_condition_formated, format_settings);

    return (full_name == other_policy.full_name)
        && (priority == other_policy.priority)
        && (to_roles == other_policy.to_roles)
        && (update_assignments_formated.str() == other_update_assignments_formated.str())
        && (where_condition_formated.str() == other_where_condition_formated.str());
}

std::vector<UUID> MaskingPolicy::findDependencies() const
{
    return to_roles.findDependencies();
}

bool MaskingPolicy::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    return to_roles.hasDependencies(ids);
}

void MaskingPolicy::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    to_roles.replaceDependencies(old_to_new_ids);
}

void MaskingPolicy::copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids)
{
    if (getType() != src.getType())
        return;
    const auto & src_policy = typeid_cast<const MaskingPolicy &>(src);
    to_roles.copyDependenciesFrom(src_policy.to_roles, ids);
}

void MaskingPolicy::removeDependencies(const std::unordered_set<UUID> & ids)
{
    to_roles.removeDependencies(ids);
}

void MaskingPolicy::clearAllExceptDependencies()
{
    update_assignments.reset();
    where_condition.reset();
    priority = 0;
}

}
