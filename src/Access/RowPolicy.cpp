#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/equal.hpp>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
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


const RowPolicy::ConditionTypeInfo & RowPolicy::ConditionTypeInfo::get(ConditionType type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        size_t underscore_pos = init_name.find('_');
        String init_command = init_name.substr(0, underscore_pos);
        boost::to_upper(init_command);
        bool init_is_check = (std::string_view{init_name}.substr(underscore_pos + 1) == "check");
        return ConditionTypeInfo{raw_name_, std::move(init_name), std::move(init_command), init_is_check};
    };

    switch (type_)
    {
        case SELECT_FILTER:
        {
            static const ConditionTypeInfo info = make_info("SELECT_FILTER");
            return info;
        }
        case MAX_CONDITION_TYPE: break;
    }
    throw Exception("Unknown type: " + std::to_string(static_cast<size_t>(type_)), ErrorCodes::LOGICAL_ERROR);
}

String toString(RowPolicy::ConditionType type)
{
    return RowPolicy::ConditionTypeInfo::get(type).raw_name;
}


String RowPolicy::NameParts::getName() const
{
    String name;
    name.reserve(database.length() + table_name.length() + short_name.length() + 6);
    name += backQuoteIfNeed(short_name);
    name += " ON ";
    if (!database.empty())
    {
        name += backQuoteIfNeed(database);
        name += '.';
    }
    name += backQuoteIfNeed(table_name);
    return name;
}

}
