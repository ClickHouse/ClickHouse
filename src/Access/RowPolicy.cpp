#include <Access/RowPolicy.h>
#include <Interpreters/Context.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/equal.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{
    void generateFullNameImpl(const String & database_, const String & table_name_, const String & policy_name_, String & full_name_)
    {
        full_name_.clear();
        full_name_.reserve(database_.length() + table_name_.length() + policy_name_.length() + 6);
        full_name_ += backQuoteIfNeed(policy_name_);
        full_name_ += " ON ";
        if (!database_.empty())
        {
            full_name_ += backQuoteIfNeed(database_);
            full_name_ += '.';
        }
        full_name_ += backQuoteIfNeed(table_name_);
    }
}


String RowPolicy::FullNameParts::getFullName() const
{
    String full_name;
    generateFullNameImpl(database, table_name, policy_name, full_name);
    return full_name;
}


String RowPolicy::FullNameParts::getFullName(const Context & context) const
{
    String full_name;
    generateFullNameImpl(database.empty() ? context.getCurrentDatabase() : database, table_name, policy_name, full_name);
    return full_name;
}


void RowPolicy::setDatabase(const String & database_)
{
    database = database_;
    generateFullNameImpl(database, table_name, policy_name, full_name);
}


void RowPolicy::setTableName(const String & table_name_)
{
    table_name = table_name_;
    generateFullNameImpl(database, table_name, policy_name, full_name);
}


void RowPolicy::setName(const String & policy_name_)
{
    policy_name = policy_name_;
    generateFullNameImpl(database, table_name, policy_name, full_name);
}


void RowPolicy::setFullName(const String & database_, const String & table_name_, const String & policy_name_)
{
    database = database_;
    table_name = table_name_;
    policy_name = policy_name_;
    generateFullNameImpl(database, table_name, policy_name, full_name);
}


bool RowPolicy::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_policy = typeid_cast<const RowPolicy &>(other);
    return (database == other_policy.database) && (table_name == other_policy.table_name) && (policy_name == other_policy.policy_name)
        && boost::range::equal(conditions, other_policy.conditions) && restrictive == other_policy.restrictive
        && (to_roles == other_policy.to_roles);
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
