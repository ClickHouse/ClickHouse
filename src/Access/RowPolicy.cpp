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
    name_parts.database = database;
    IAccessEntity::setName(name_parts.getName());
}

void RowPolicy::setTableName(const String & table_name)
{
    name_parts.table_name = table_name;
    IAccessEntity::setName(name_parts.getName());
}

void RowPolicy::setShortName(const String & short_name)
{
    name_parts.short_name = short_name;
    IAccessEntity::setName(name_parts.getName());
}

void RowPolicy::setNameParts(const String & short_name, const String & database, const String & table_name)
{
    name_parts.short_name = short_name;
    name_parts.database = database;
    name_parts.table_name = table_name;
    IAccessEntity::setName(name_parts.getName());
}

void RowPolicy::setNameParts(const NameParts & name_parts_)
{
    name_parts = name_parts_;
    IAccessEntity::setName(name_parts.getName());
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
    return (name_parts == other_policy.name_parts) && boost::range::equal(conditions, other_policy.conditions)
        && restrictive == other_policy.restrictive && (to_roles == other_policy.to_roles);
}

}
