#include <algorithm>
#include <limits>
#include <Common/StringUtils.h>
#include <Access/Common/QuotaDefs.h>
#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <Core/AccurateComparison.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    return toString(static_cast<double>(value) / static_cast<double>(output_denominator));
}

QuotaValue QuotaTypeInfo::stringToValue(const String & str) const
{
    if (output_denominator == 1)
        return static_cast<QuotaValue>(parse<UInt64>(str));
    return scaleToValue(parse<Float64>(str));
}

QuotaValue QuotaTypeInfo::scaleToValue(Float64 unscaled) const
{
    const Float64 scaled = unscaled * static_cast<Float64>(output_denominator);

    /// Casting a `Float64` that does not fit into `QuotaValue` is undefined behavior, so the range is checked first.
    /// `accurate::greaterOp` is used instead of a naive comparison because `Float64(std::numeric_limits<QuotaValue>::max())`
    /// rounds up, so a naive comparison still lets through values that are out of range - the same reasoning as in
    /// `FieldVisitorConvertToNumber`.
    if (!isFinite(scaled)
        || accurate::greaterOp(scaled, std::numeric_limits<QuotaValue>::max())
        || accurate::lessOp(scaled, std::numeric_limits<QuotaValue>::lowest()))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value {} is out of range for the quota type {}", unscaled, name);
    }

    return static_cast<QuotaValue>(scaled);
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
        toLowerASCII(init_name);
        String init_keyword = raw_name_;
        std::replace(init_keyword.begin(), init_keyword.end(), '_', ' ');
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
        case QuotaType::QUERIES_PER_NORMALIZED_HASH:
        {
            static const auto info = make_info(
                "QUERIES_PER_NORMALIZED_HASH",
                "The current maximum number of executions of any single normalized query within the current period of time.",
                "The maximum number of executions of any single normalized query allowed within the specified period of time.",
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
        toLowerASCII(init_name);
        std::vector<QuotaKeyType> init_base_types;
        /// The name of a composite key type is its parts joined with "_or_".
        Strings tokens;
        for (size_t begin = 0; begin <= init_name.length();)
        {
            size_t end = init_name.find("_or_", begin);
            size_t token_end = (end == String::npos) ? init_name.length() : end;
            tokens.emplace_back(init_name.substr(begin, token_end - begin));
            if (end == String::npos)
                break;
            begin = end + 4;
        }
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
        case QuotaKeyType::NORMALIZED_QUERY_HASH:
        {
            static const auto info = make_info("NORMALIZED_QUERY_HASH");
            return info;
        }
        case QuotaKeyType::MAX: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected quota key type: {}", static_cast<int>(type));
}

}
