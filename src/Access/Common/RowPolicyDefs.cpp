#include <Access/Common/RowPolicyDefs.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String RowPolicyName::toString() const
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

String toString(RowPolicyFilterType type)
{
    return RowPolicyFilterTypeInfo::get(type).raw_name;
}

const RowPolicyFilterTypeInfo & RowPolicyFilterTypeInfo::get(RowPolicyFilterType type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        size_t underscore_pos = init_name.find('_');
        String init_command = init_name.substr(0, underscore_pos);
        boost::to_upper(init_command);
        bool init_is_check = (std::string_view{init_name}.substr(underscore_pos + 1) == "check");
        return RowPolicyFilterTypeInfo{raw_name_, std::move(init_name), std::move(init_command), init_is_check};
    };

    switch (type_)
    {
        case RowPolicyFilterType::SELECT_FILTER:
        {
            static const auto info = make_info("SELECT_FILTER");
            return info;
        }
#if 0 /// Row-level security for INSERT, UPDATE, DELETE is not implemented yet.
        case RowPolicyFilterType::INSERT_CHECK:
        {
            static const auto info = make_info("INSERT_CHECK");
            return info;
        }
        case RowPolicyFilterType::UPDATE_FILTER:
        {
            static const auto info = make_info("UPDATE_FILTER");
            return info;
        }
        case RowPolicyFilterType::UPDATE_CHECK:
        {
            static const auto info = make_info("UPDATE_CHECK");
            return info;
        }
        case RowPolicyFilterType::DELETE_FILTER:
        {
            static const auto info = make_info("DELETE_FILTER");
            return info;
        }
#endif
        case RowPolicyFilterType::MAX: break;
    }
    throw Exception("Unknown type: " + std::to_string(static_cast<size_t>(type_)), ErrorCodes::LOGICAL_ERROR);
}

}
