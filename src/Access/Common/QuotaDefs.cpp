#include <Access/Common/QuotaDefs.h>
#include <Common/Exception.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String QuotaTypeInfo::valueToString(QuotaValue value) const
{
    if (!(value % output_denominator))
        return std::to_string(value / output_denominator);
    else
        return boost::lexical_cast<std::string>(static_cast<double>(value) / output_denominator);
}

QuotaValue QuotaTypeInfo::stringToValue(const String & str) const
{
    if (output_denominator == 1)
        return static_cast<QuotaValue>(std::strtoul(str.c_str(), nullptr, 10));
    else
        return static_cast<QuotaValue>(std::strtod(str.c_str(), nullptr) * output_denominator);
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
    static constexpr auto make_info = [](const char * raw_name_, UInt64 output_denominator_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        String init_keyword = raw_name_;
        boost::replace_all(init_keyword, "_", " ");
        bool init_output_as_float = (output_denominator_ != 1);
        return QuotaTypeInfo{raw_name_, std::move(init_name), std::move(init_keyword), init_output_as_float, output_denominator_};
    };

    switch (type)
    {
        case QuotaType::QUERIES:
        {
            static const auto info = make_info("QUERIES", 1);
            return info;
        }
        case QuotaType::QUERY_SELECTS:
        {
            static const auto info = make_info("QUERY_SELECTS", 1);
            return info;
        }
        case QuotaType::QUERY_INSERTS:
        {
            static const auto info = make_info("QUERY_INSERTS", 1);
            return info;
        }
        case QuotaType::ERRORS:
        {
            static const auto info = make_info("ERRORS", 1);
            return info;
        }
        case QuotaType::RESULT_ROWS:
        {
            static const auto info = make_info("RESULT_ROWS", 1);
            return info;
        }
        case QuotaType::RESULT_BYTES:
        {
            static const auto info = make_info("RESULT_BYTES", 1);
            return info;
        }
        case QuotaType::READ_ROWS:
        {
            static const auto info = make_info("READ_ROWS", 1);
            return info;
        }
        case QuotaType::READ_BYTES:
        {
            static const auto info = make_info("READ_BYTES", 1);
            return info;
        }
        case QuotaType::EXECUTION_TIME:
        {
            static const auto info = make_info("EXECUTION_TIME", 1000000000 /* execution_time is stored in nanoseconds */);
            return info;
        }
        case QuotaType::MAX: break;
    }
    throw Exception("Unexpected quota type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}

}
