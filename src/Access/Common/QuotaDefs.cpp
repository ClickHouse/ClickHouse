#include <Access/Common/QuotaDefs.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


String toString(QuotaType type)
{
    return QuotaTypeInfo::get(type).raw_name;
}

String QuotaTypeInfo::valueToString(QuotaValue value) const
{
    if (!(value % output_denominator))
        return std::to_string(value / output_denominator);
    return toString(static_cast<double>(value) / output_denominator);
}

QuotaValue QuotaTypeInfo::stringToValue(const String & str) const
{
    if (output_denominator == 1)
        return static_cast<QuotaValue>(parse<UInt64>(str));
    return static_cast<QuotaValue>(parse<Float64>(str) * output_denominator);
}

String QuotaTypeInfo::valueToStringWithName(QuotaValue value) const
{
    String res = name;
    res += " = ";
    res += valueToString(value);
    return res;
}

const QuotaTypeInfo & QuotaTypeInfo::get(QuotaType type)
{
    static constexpr auto make_info = [](const char * raw_name_, String current_usage_description_, String max_allowed_usage_description_, UInt64 output_denominator_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        String init_keyword = raw_name_;
        boost::replace_all(init_keyword, "_", " ");
        bool init_output_as_float = (output_denominator_ != 1);
        return QuotaTypeInfo
        {
            .raw_name = raw_name_,
            .name = std::move(init_name),
            .keyword = std::move(init_keyword),
            .current_usage_description = std::move(current_usage_description_),
            .max_allowed_usage_description = std::move(max_allowed_usage_description_),
            .output_as_float = init_output_as_float,
            .output_denominator = output_denominator_
        };
    };

    switch (type)
    {
        case QuotaType::QUERIES:
        {
            static const auto info = make_info(
                "QUERIES",
                "The current number of executed queries.",
                "The maximum allowed number of queries of all types allowed to be executed.",
                1
            );
            return info;
        }
        case QuotaType::QUERY_SELECTS:
        {
            static const auto info = make_info(
                "QUERY_SELECTS",
                "The current number of executed SELECT queries.",
                "The maximum allowed number of SELECT queries allowed to be executed.",
                1
            );
            return info;
        }
        case QuotaType::QUERY_INSERTS:
        {
            static const auto info = make_info(
                "QUERY_INSERTS",
                "The current number of executed INSERT queries.",
                "The maximum allowed number of INSERT queries allowed to be executed.",
                1
            );
            return info;
        }
        case QuotaType::ERRORS:
        {
            static const auto info = make_info(
                "ERRORS",
                "The current number of queries resulted in an error.",
                "The maximum number of queries resulted in an error allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::RESULT_ROWS:
        {
            static const auto info = make_info(
                "RESULT_ROWS",
                "The current total number of rows in the result set of all queries within the current period of time.",
                "The maximum total number of rows in the result set of all queries allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::RESULT_BYTES:
        {
            static const auto info = make_info(
                "RESULT_BYTES",
                "The current total number of bytes in the result set of all queries within the current period of time.",
                "The maximum total number of bytes in the result set of all queries allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::READ_ROWS:
        {
            static const auto info = make_info(
                "READ_ROWS",
                "The current total number of rows read during execution of all queries within the current period of time.",
                "The maximum number of rows to read during execution of all queries allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::READ_BYTES:
        {
            static const auto info = make_info(
                "READ_BYTES",
                "The current total number of bytes read during execution of all queries within the current period of time.",
                "The maximum number of bytes to read during execution of all queries allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::EXECUTION_TIME:
        {
            static const auto info = make_info(
                "EXECUTION_TIME",
                "The current total amount of time (in nanoseconds) spent to execute queries within the current period of time",
                "The maximum amount of time (in nanoseconds) allowed for all queries to execute within the specified period of time",
                1000000000 /* execution_time is stored in nanoseconds */
            );
            return info;
        }
        case QuotaType::WRITTEN_BYTES:
        {
            static const auto info = make_info(
                "WRITTEN_BYTES",
                "The current total number of bytes written during execution of all queries within the current period of time.",
                "The maximum number of bytes to written during execution of all queries allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::FAILED_SEQUENTIAL_AUTHENTICATIONS:
        {
            static const auto info = make_info(
                "FAILED_SEQUENTIAL_AUtheNTICATIONS",
                "The current number of consecutive authentication failures within the current period of time.",
                "The maximum number of consecutive authentication failures allowed within the specified period of time.",
                1
            );
            return info;
        }
        case QuotaType::MAX: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected quota type: {}", static_cast<int>(type));
}

String toString(QuotaKeyType type)
{
    return QuotaKeyTypeInfo::get(type).raw_name;
}

const QuotaKeyTypeInfo & QuotaKeyTypeInfo::get(QuotaKeyType type)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        std::vector<QuotaKeyType> init_base_types;
        String replaced = boost::algorithm::replace_all_copy(init_name, "_or_", "|");
        Strings tokens;
        boost::algorithm::split(tokens, replaced, boost::is_any_of("|"));
        if (tokens.size() > 1)
        {
            for (const auto & token : tokens)
            {
                for (auto kt : collections::range(QuotaKeyType::MAX))
                {
                    if (QuotaKeyTypeInfo::get(kt).name == token)
                    {
                        init_base_types.push_back(kt);
                        break;
                    }
                }
            }
        }
        return QuotaKeyTypeInfo{raw_name_, std::move(init_name), std::move(init_base_types)};
    };

    switch (type)
    {
        case QuotaKeyType::NONE:
        {
            static const auto info = make_info("NONE");
            return info;
        }
        case QuotaKeyType::USER_NAME:
        {
            static const auto info = make_info("USER_NAME");
            return info;
        }
        case QuotaKeyType::IP_ADDRESS:
        {
            static const auto info = make_info("IP_ADDRESS");
            return info;
        }
        case QuotaKeyType::FORWARDED_IP_ADDRESS:
        {
            static const auto info = make_info("FORWARDED_IP_ADDRESS");
            return info;
        }
        case QuotaKeyType::CLIENT_KEY:
        {
            static const auto info = make_info("CLIENT_KEY");
            return info;
        }
        case QuotaKeyType::CLIENT_KEY_OR_USER_NAME:
        {
            static const auto info = make_info("CLIENT_KEY_OR_USER_NAME");
            return info;
        }
        case QuotaKeyType::CLIENT_KEY_OR_IP_ADDRESS:
        {
            static const auto info = make_info("CLIENT_KEY_OR_IP_ADDRESS");
            return info;
        }
        case QuotaKeyType::MAX: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected quota key type: {}", static_cast<int>(type));
}

}
