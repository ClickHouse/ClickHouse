#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/equal.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


void RowPolicy::setDatabase(const String & database)
{
    full_name.database = database;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setTableName(const String & table_name)
{
    full_name.table_name = table_name;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setShortName(const String & short_name)
{
    full_name.short_name = short_name;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setFullName(const String & short_name, const String & database, const String & table_name)
{
    full_name.short_name = short_name;
    full_name.database = database;
    full_name.table_name = table_name;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setFullName(const RowPolicyName & full_name_)
{
    full_name = full_name_;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setName(const String &)
{
    throw Exception("RowPolicy::setName() is not implemented", ErrorCodes::NOT_IMPLEMENTED);
}


bool RowPolicy::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_policy = typeid_cast<const RowPolicy &>(other);
    return (full_name == other_policy.full_name) && boost::range::equal(filters, other_policy.filters)
        && restrictive == other_policy.restrictive && (to_roles == other_policy.to_roles);
}

}
