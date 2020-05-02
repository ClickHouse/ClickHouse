#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/equal.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}


String RowPolicy::NameParts::getName() const
{
    String name;
    name.reserve(database.length() + table_name.length() + short_name.length() + 6);
    name += backQuoteIfNeed(short_name);
    name += " ON ";
    if (!name.empty())
    {
        name += backQuoteIfNeed(database);
        name += '.';
    }
    name += backQuoteIfNeed(table_name);
    return name;
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


const char * RowPolicy::conditionTypeToString(ConditionType index)
{
    switch (index)
    {
        case SELECT_FILTER: return "SELECT_FILTER";
        case INSERT_CHECK: return "INSERT_CHECK";
        case UPDATE_FILTER: return "UPDATE_FILTER";
        case UPDATE_CHECK: return "UPDATE_CHECK";
        case DELETE_FILTER: return "DELETE_FILTER";
        case MAX_CONDITION_TYPE: break;
    }
    throw Exception("Unexpected condition type: " + std::to_string(static_cast<int>(index)), ErrorCodes::LOGICAL_ERROR);
}


const char * RowPolicy::conditionTypeToColumnName(ConditionType index)
{
    switch (index)
    {
        case SELECT_FILTER: return "select_filter";
        case INSERT_CHECK: return "insert_check";
        case UPDATE_FILTER: return "update_filter";
        case UPDATE_CHECK: return "update_check";
        case DELETE_FILTER: return "delete_filter";
        case MAX_CONDITION_TYPE: break;
    }
    throw Exception("Unexpected condition type: " + std::to_string(static_cast<int>(index)), ErrorCodes::LOGICAL_ERROR);
}

}
