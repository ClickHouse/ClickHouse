#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <Common/Exception.h>
#include <Common/isValidUTF8.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>
#include <base/hex.h>
#include <Poco/Unicode.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <charconv>
#include <initializer_list>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    using Node = PrometheusQueryTree::Node;
    using ResultType = PrometheusQueryTree::ResultType;

    bool isOneOf(std::string_view value, std::initializer_list<std::string_view> values)
    {
        for (const auto candidate : values)
        {
            if (value == candidate)
                return true;
        }
        return false;
    }

    void checkArgumentCount(std::string_view kind, std::string_view name, size_t actual, size_t expected)
    {
        if (actual != expected)
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "{} '{}' expects {} arguments, but was called with {} arguments",
                            kind, name, expected, actual);
        }
    }

    void checkArgumentCountAtLeast(std::string_view kind, std::string_view name, size_t actual, size_t expected)
    {
        if (actual < expected)
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "{} '{}' expects at least {} arguments, but was called with {} arguments",
                            kind, name, expected, actual);
        }
    }

    void checkArgumentType(
        std::string_view kind,
        std::string_view name,
        const std::vector<const Node *> & arguments,
        size_t argument_index,
        ResultType expected)
    {
        const auto actual = arguments[argument_index]->result_type;
        if (actual != expected)
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "{} '{}' expects argument #{} of type {}, but got {}",
                            kind, name, argument_index + 1, expected, actual);
        }
    }

    void checkArgumentTypes(
        std::string_view kind,
        std::string_view name,
        const std::vector<const Node *> & arguments,
        std::initializer_list<ResultType> expected_types)
    {
        size_t argument_index = 0;
        for (const auto expected : expected_types)
        {
            checkArgumentType(kind, name, arguments, argument_index, expected);
            ++argument_index;
        }
    }

    bool isScalarOrInstantVector(ResultType type)
    {
        return type == ResultType::SCALAR || type == ResultType::INSTANT_VECTOR;
    }

    void validateFunction(const PrometheusQueryTree::Function & function)
    {
        const auto & name = function.function_name;
        const auto & arguments = function.getArguments();

        if (isOneOf(name, {"time", "pi"}))
        {
            checkArgumentCount("Function", name, arguments.size(), 0);
        }
        else if (name == "scalar")
        {
            checkArgumentCount("Function", name, arguments.size(), 1);
            checkArgumentTypes("Function", name, arguments, {ResultType::INSTANT_VECTOR});
        }
        else if (name == "vector")
        {
            checkArgumentCount("Function", name, arguments.size(), 1);
            checkArgumentTypes("Function", name, arguments, {ResultType::SCALAR});
        }
        else if (isOneOf(name, {
                     "abs", "absent", "ceil", "floor", "histogram_count", "histogram_sum", "sort", "sort_desc", "timestamp",
                     "acos", "acosh", "asin", "asinh", "atan", "atanh", "cos", "cosh", "deg", "exp", "ln", "log2",
                     "log10", "rad", "sgn", "sin", "sinh", "sqrt", "tan", "tanh"}))
        {
            checkArgumentCount("Function", name, arguments.size(), 1);
            checkArgumentTypes("Function", name, arguments, {ResultType::INSTANT_VECTOR});
        }
        else if (isOneOf(name, {
                     "absent_over_time", "avg_over_time", "changes", "count_over_time", "delta", "deriv", "idelta", "increase",
                     "irate", "last_over_time", "max_over_time", "min_over_time", "present_over_time", "rate", "resets",
                     "stddev_over_time", "stdvar_over_time", "sum_over_time"}))
        {
            checkArgumentCount("Function", name, arguments.size(), 1);
            checkArgumentTypes("Function", name, arguments, {ResultType::RANGE_VECTOR});
        }
        else if (isOneOf(name, {"day_of_month", "day_of_week", "day_of_year", "days_in_month", "hour", "minute", "month", "year"}))
        {
            if (arguments.size() > 1)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Function '{}' expects 0 or 1 arguments, but was called with {} arguments",
                                name, arguments.size());
            }
            if (!arguments.empty())
                checkArgumentTypes("Function", name, arguments, {ResultType::INSTANT_VECTOR});
        }
        else if (name == "clamp")
        {
            checkArgumentCount("Function", name, arguments.size(), 3);
            checkArgumentTypes("Function", name, arguments, {ResultType::INSTANT_VECTOR, ResultType::SCALAR, ResultType::SCALAR});
        }
        else if (isOneOf(name, {"clamp_min", "clamp_max"}))
        {
            checkArgumentCount("Function", name, arguments.size(), 2);
            checkArgumentTypes("Function", name, arguments, {ResultType::INSTANT_VECTOR, ResultType::SCALAR});
        }
        else if (name == "round")
        {
            if (arguments.size() != 1 && arguments.size() != 2)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Function '{}' expects 1 or 2 arguments, but was called with {} arguments",
                                name, arguments.size());
            }
            checkArgumentType("Function", name, arguments, 0, ResultType::INSTANT_VECTOR);
            if (arguments.size() == 2)
                checkArgumentType("Function", name, arguments, 1, ResultType::SCALAR);
        }
        else if (name == "label_replace")
        {
            checkArgumentCount("Function", name, arguments.size(), 5);
            checkArgumentType("Function", name, arguments, 0, ResultType::INSTANT_VECTOR);
            for (size_t i = 1; i < arguments.size(); ++i)
                checkArgumentType("Function", name, arguments, i, ResultType::STRING);
        }
        else if (name == "label_join")
        {
            checkArgumentCountAtLeast("Function", name, arguments.size(), 3);
            checkArgumentType("Function", name, arguments, 0, ResultType::INSTANT_VECTOR);
            for (size_t i = 1; i < arguments.size(); ++i)
                checkArgumentType("Function", name, arguments, i, ResultType::STRING);
        }
        else if (name == "histogram_quantile")
        {
            checkArgumentCount("Function", name, arguments.size(), 2);
            checkArgumentTypes("Function", name, arguments, {ResultType::SCALAR, ResultType::INSTANT_VECTOR});
        }
        else if (name == "histogram_fraction")
        {
            checkArgumentCount("Function", name, arguments.size(), 3);
            checkArgumentTypes("Function", name, arguments, {ResultType::SCALAR, ResultType::SCALAR, ResultType::INSTANT_VECTOR});
        }
        else if (name == "holt_winters")
        {
            checkArgumentCount("Function", name, arguments.size(), 3);
            checkArgumentTypes("Function", name, arguments, {ResultType::RANGE_VECTOR, ResultType::SCALAR, ResultType::SCALAR});
        }
        else if (name == "predict_linear")
        {
            checkArgumentCount("Function", name, arguments.size(), 2);
            checkArgumentTypes("Function", name, arguments, {ResultType::RANGE_VECTOR, ResultType::SCALAR});
        }
        else if (name == "quantile_over_time")
        {
            checkArgumentCount("Function", name, arguments.size(), 2);
            checkArgumentTypes("Function", name, arguments, {ResultType::SCALAR, ResultType::RANGE_VECTOR});
        }
    }

    void validateAggregationOperator(const PrometheusQueryTree::AggregationOperator & aggregation)
    {
        const auto & name = aggregation.operator_name;
        const auto & arguments = aggregation.getArguments();

        if (isOneOf(name, {"sum", "min", "max", "avg", "count", "stddev", "stdvar", "group"}))
        {
            checkArgumentCount("Aggregation operator", name, arguments.size(), 1);
            checkArgumentTypes("Aggregation operator", name, arguments, {ResultType::INSTANT_VECTOR});
        }
        else if (name == "count_values")
        {
            checkArgumentCount("Aggregation operator", name, arguments.size(), 2);
            checkArgumentTypes("Aggregation operator", name, arguments, {ResultType::STRING, ResultType::INSTANT_VECTOR});
        }
        else if (isOneOf(name, {"bottomk", "limitk", "topk", "quantile"}))
        {
            checkArgumentCount("Aggregation operator", name, arguments.size(), 2);
            checkArgumentTypes("Aggregation operator", name, arguments, {ResultType::SCALAR, ResultType::INSTANT_VECTOR});
        }
    }

    void validateBinaryOperator(const PrometheusQueryTree::BinaryOperator & binary)
    {
        const auto left_type = binary.getLeftArgument()->result_type;
        const auto right_type = binary.getRightArgument()->result_type;
        const bool both_vectors = left_type == ResultType::INSTANT_VECTOR && right_type == ResultType::INSTANT_VECTOR;
        const bool has_vector_matching = binary.on || binary.ignoring || binary.group_left || binary.group_right;

        if (binary.operator_name == "and" || binary.operator_name == "or" || binary.operator_name == "unless")
        {
            if (!both_vectors)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Binary operator '{}' expects two arguments of type {}, but got {} and {}",
                                binary.operator_name, ResultType::INSTANT_VECTOR, left_type, right_type);
            }
            if (binary.bool_modifier)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Binary operator '{}' doesn't allow the bool modifier", binary.operator_name);
            }
            if (binary.group_left || binary.group_right)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Binary operator '{}' doesn't allow group modifiers", binary.operator_name);
            }
            return;
        }

        if (!isScalarOrInstantVector(left_type) || !isScalarOrInstantVector(right_type))
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Binary operator '{}' expects arguments of type {} or {}, but got {} and {}",
                            binary.operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR, left_type, right_type);
        }

        const bool is_comparison = isOneOf(binary.operator_name, {"==", "!=", ">", "<", ">=", "<="});
        if (is_comparison)
        {
            if (left_type == ResultType::SCALAR && right_type == ResultType::SCALAR && !binary.bool_modifier)
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Comparison operator '{}' on scalars requires the bool modifier", binary.operator_name);
            }
        }
        else if (binary.bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Binary operator '{}' doesn't allow the bool modifier", binary.operator_name);
        }

        if (has_vector_matching && !both_vectors)
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Vector matching for binary operator '{}' requires two instant-vector arguments, but got {} and {}",
                            binary.operator_name, left_type, right_type);
        }
    }

    void validateNode(const Node * node)
    {
        for (const auto * child : node->children)
            validateNode(child);

        switch (node->node_type)
        {
            case PrometheusQueryTree::NodeType::Subquery:
            {
                const auto * subquery = static_cast<const PrometheusQueryTree::Subquery *>(node);
                if (subquery->getExpression()->result_type != ResultType::INSTANT_VECTOR)
                {
                    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                    "Subquery expression must have type {}, but got {}",
                                    ResultType::INSTANT_VECTOR, subquery->getExpression()->result_type);
                }
                break;
            }
            case PrometheusQueryTree::NodeType::Offset:
            {
                const auto * offset = static_cast<const PrometheusQueryTree::Offset *>(node);
                if (offset->getExpression()->result_type != ResultType::INSTANT_VECTOR
                    && offset->getExpression()->result_type != ResultType::RANGE_VECTOR)
                {
                    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                    "Offset expression must have type {} or {}, but got {}",
                                    ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR, offset->getExpression()->result_type);
                }
                break;
            }
            case PrometheusQueryTree::NodeType::Function:
                validateFunction(static_cast<const PrometheusQueryTree::Function &>(*node));
                break;
            case PrometheusQueryTree::NodeType::UnaryOperator:
            {
                const auto * unary = static_cast<const PrometheusQueryTree::UnaryOperator *>(node);
                if (unary->operator_name != "+" && unary->operator_name != "-")
                {
                    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                    "Unknown unary operator '{}'", unary->operator_name);
                }
                if (!isScalarOrInstantVector(unary->getArgument()->result_type))
                {
                    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                    "Unary operator '{}' expects an argument of type {} or {}, but got {}",
                                    unary->operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                                    unary->getArgument()->result_type);
                }
                break;
            }
            case PrometheusQueryTree::NodeType::BinaryOperator:
                validateBinaryOperator(static_cast<const PrometheusQueryTree::BinaryOperator &>(*node));
                break;
            case PrometheusQueryTree::NodeType::AggregationOperator:
                validateAggregationOperator(static_cast<const PrometheusQueryTree::AggregationOperator &>(*node));
                break;
            case PrometheusQueryTree::NodeType::Scalar:
            case PrometheusQueryTree::NodeType::StringLiteral:
            case PrometheusQueryTree::NodeType::InstantSelector:
            case PrometheusQueryTree::NodeType::RangeSelector:
                break;
        }
    }

    String quotePromQLString(std::string_view str)
    {
        String result;
        result.reserve(str.size() + 2);
        result.push_back('"');

        for (size_t i = 0; i < str.size();)
        {
            const auto c = static_cast<UInt8>(str[i]);

            if (c >= 0x80)
            {
                const size_t sequence_length = UTF8::seqLength(c);
                if (sequence_length <= str.size() - i
                    && UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data() + i), sequence_length))
                {
                    result.append(str.data() + i, sequence_length);
                    i += sequence_length;
                    continue;
                }
            }

            switch (c)
            {
                case '"':
                case '\\':
                    result.push_back('\\');
                    result.push_back(static_cast<char>(c));
                    break;
                case '\a': result.append("\\a"); break;
                case '\b': result.append("\\b"); break;
                case '\f': result.append("\\f"); break;
                case '\n': result.append("\\n"); break;
                case '\r': result.append("\\r"); break;
                case '\t': result.append("\\t"); break;
                case '\v': result.append("\\v"); break;
                default:
                    if (c < 0x20 || c == 0x7F || c >= 0x80)
                    {
                        result.append("\\x");
                        result += getHexUIntLowercase(c);
                    }
                    else
                    {
                        result.push_back(static_cast<char>(c));
                    }
                    break;
            }

            ++i;
        }

        result.push_back('"');
        return result;
    }

    bool isLegacyLabelName(std::string_view label)
    {
        if (label.empty())
            return false;

        if (!isAlphaASCII(label.front()) && label.front() != '_')
            return false;

        for (char c : label.substr(1))
        {
            if (!isAlphaNumericASCII(c) && c != '_')
                return false;
        }

        return true;
    }

    bool isLegacyMetricName(std::string_view metric)
    {
        if (metric.empty())
            return false;

        if (!isAlphaASCII(metric.front()) && metric.front() != '_' && metric.front() != ':')
            return false;

        for (char c : metric.substr(1))
        {
            if (!isAlphaNumericASCII(c) && c != '_' && c != ':')
                return false;
        }

        return true;
    }

    /// Keywords which cannot be used as a bare metric name because a parser reading them
    /// in the position of an operand would treat them as a binary operator or a modifier.
    /// For example, `foo * on` is not a multiplication of `foo` and the metric `on`,
    /// it's an incomplete `on (...)` matching modifier. Such names must stay quoted.
    bool isReservedKeyword(std::string_view name)
    {
        static constexpr std::string_view keywords[]
            = {"and", "or", "unless", "atan2", "by", "without", "on", "ignoring", "group_left", "group_right", "offset", "bool"};

        for (const auto & keyword : keywords)
        {
            if (equalsCaseInsensitive(name, keyword))
                return true;
        }

        return false;
    }

    bool canPrintLabelNameUnquoted(std::string_view label)
    {
        return isLegacyLabelName(label)
            && !equalsCaseInsensitive(label, "inf")
            && !equalsCaseInsensitive(label, "nan");
    }

    bool canPrintMetricNameUnquoted(std::string_view metric)
    {
        return isLegacyMetricName(metric)
            && !equalsCaseInsensitive(metric, "inf")
            && !equalsCaseInsensitive(metric, "nan")
            && !isReservedKeyword(metric);
    }

    String formatLabelName(const String & label)
    {
        return canPrintLabelNameUnquoted(label) ? label : quotePromQLString(label);
    }

    String formatMetricName(const String & metric)
    {
        return canPrintMetricNameUnquoted(metric) ? metric : quotePromQLString(metric);
    }

    /// Prometheus uses strconv.Quote for strings, including Unicode printability rules.
    bool isPrometheusPrintable(UInt32 code_point)
    {
        if (code_point <= 0xFF)
            return (code_point >= 0x20 && code_point <= 0x7E) || (code_point >= 0xA1 && code_point != 0xAD);

        Poco::Unicode::CharacterProperties properties{};
        Poco::Unicode::properties(code_point, properties);
        return properties.category == Poco::Unicode::UCP_LETTER
            || properties.category == Poco::Unicode::UCP_MARK
            || properties.category == Poco::Unicode::UCP_NUMBER
            || properties.category == Poco::Unicode::UCP_PUNCTUATION
            || properties.category == Poco::Unicode::UCP_SYMBOL;
    }

    String quotePrometheusString(std::string_view str)
    {
        static constexpr char hex_digits[] = "0123456789abcdef";

        String result;
        result.reserve(str.size() + 2);
        result += '"';

        const auto * data = reinterpret_cast<const UInt8 *>(str.data());
        for (size_t i = 0; i < str.size();)
        {
            const UInt8 c = data[i];
            switch (c)
            {
                case '"': result += "\\\""; ++i; break;
                case '\\': result += "\\\\"; ++i; break;
                case '\a': result += "\\a"; ++i; break;
                case '\b': result += "\\b"; ++i; break;
                case '\f': result += "\\f"; ++i; break;
                case '\n': result += "\\n"; ++i; break;
                case '\r': result += "\\r"; ++i; break;
                case '\t': result += "\\t"; ++i; break;
                case '\v': result += "\\v"; ++i; break;
                default:
                {
                    if (c < 0x20 || c == 0x7F)
                    {
                        result += "\\x";
                        result += hex_digits[c >> 4];
                        result += hex_digits[c & 0x0F];
                        ++i;
                    }
                    else if (c >= 0x80)
                    {
                        const size_t sequence_length = UTF8::seqLength(c);
                        if (sequence_length <= str.size() - i
                            && UTF8::isValidUTF8(data + i, sequence_length))
                        {
                            const auto code_point = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(data + i), sequence_length);
                            if (code_point && isPrometheusPrintable(*code_point))
                                result.append(str, i, sequence_length);
                            else if (code_point && *code_point <= 0xFFFF)
                                result += fmt::format("\\u{:04x}", *code_point);
                            else if (code_point)
                                result += fmt::format("\\U{:08x}", *code_point);
                            else
                            {
                                result += "\\x";
                                result += hex_digits[c >> 4];
                                result += hex_digits[c & 0x0F];
                            }
                            i += sequence_length;
                        }
                        else
                        {
                            result += "\\x";
                            result += hex_digits[c >> 4];
                            result += hex_digits[c & 0x0F];
                            ++i;
                        }
                    }
                    else
                    {
                        result += static_cast<char>(c);
                        ++i;
                    }
                    break;
                }
            }
        }

        result += '"';
        return result;
    }

    String formatPrometheusLabelName(const String & label)
    {
        return isLegacyLabelName(label) ? label : quotePrometheusString(label);
    }

    String formatPrometheusMetricName(const String & metric)
    {
        return canPrintMetricNameUnquoted(metric) ? metric : quotePrometheusString(metric);
    }

    UInt64 unsignedAbsoluteValue(Int64 value)
    {
        if (value >= 0)
            return static_cast<UInt64>(value);
        return static_cast<UInt64>(-(value + 1)) + 1;
    }

    Int64 decimalToMilliseconds(Decimal64 value, UInt32 scale)
    {
        if (scale >= 3)
            return value.value / DecimalUtils::scaleMultiplier<Decimal64>(scale - 3);

        return value.value * DecimalUtils::scaleMultiplier<Decimal64>(3 - scale);
    }

    Int64 decimalToRoundedMilliseconds(Decimal64 value, UInt32 scale)
    {
        const Int64 milliseconds = decimalToMilliseconds(value, scale);
        if (scale <= 3)
            return milliseconds;

        const Int64 divisor = DecimalUtils::scaleMultiplier<Decimal64>(scale - 3);
        const Int64 remainder = value.value % divisor;
        if (unsignedAbsoluteValue(remainder) * 2 < static_cast<UInt64>(divisor))
            return milliseconds;

        return milliseconds + (value.value < 0 ? -1 : 1);
    }

    String formatPrometheusTimestamp(const DateTime64 & timestamp, UInt32 scale)
    {
        const Int64 milliseconds = decimalToRoundedMilliseconds(Decimal64{timestamp}, scale);
        const UInt64 absolute = unsignedAbsoluteValue(milliseconds);
        return fmt::format("{}{}.{:03}", milliseconds < 0 ? "-" : "", absolute / 1000, absolute % 1000);
    }

    String formatPrometheusDuration(Decimal64 duration, UInt32 scale)
    {
        Int64 milliseconds = decimalToMilliseconds(duration, scale);
        if (milliseconds == 0)
            return "0s";

        const bool negative = milliseconds < 0;
        UInt64 remaining = unsignedAbsoluteValue(milliseconds);
        String result;

        auto append_unit = [&](std::string_view unit, UInt64 multiplier, bool exact)
        {
            if (exact && remaining % multiplier != 0)
                return;

            const UInt64 value = remaining / multiplier;
            if (value == 0)
                return;

            result += fmt::format("{}{}", value, unit);
            remaining -= value * multiplier;
        };

        /// This follows model.Duration.String(): years and weeks are only used
        /// when they consume the whole duration, which keeps 90d readable.
        append_unit("y", 365ULL * 24 * 60 * 60 * 1000, true);
        append_unit("w", 7ULL * 24 * 60 * 60 * 1000, true);
        append_unit("d", 24ULL * 60 * 60 * 1000, false);
        append_unit("h", 60ULL * 60 * 1000, false);
        append_unit("m", 60ULL * 1000, false);
        append_unit("s", 1000, false);
        append_unit("ms", 1, false);

        if (negative)
            result.insert(result.begin(), '-');
        return result;
    }

    String formatPrometheusScalar(const PrometheusQueryTree::Scalar & scalar)
    {
        if (scalar.duration_value)
            return formatPrometheusDuration(*scalar.duration_value, /* scale */ 3);

        if (std::isinf(scalar.scalar))
            return scalar.scalar < 0 ? "-Inf" : "+Inf";
        if (std::isnan(scalar.scalar))
            return "NaN";

        char buffer[1100];
        const auto [end, error] = std::to_chars(buffer, buffer + sizeof(buffer), scalar.scalar, std::chars_format::fixed);
        if (error != std::errc{})
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot format Prometheus scalar {}", scalar.scalar);
        return {buffer, end};
    }

    using PrometheusMatcherType = PrometheusQueryTree::MatcherType;

    String formatPrometheusMatcher(const PrometheusQueryTree::Matcher & matcher)
    {
        String result = formatPrometheusLabelName(matcher.label_name);
        switch (matcher.matcher_type)
        {
            case PrometheusMatcherType::EQ:  result += '=';  break;
            case PrometheusMatcherType::NE:  result += "!="; break;
            case PrometheusMatcherType::RE:  result += "=~"; break;
            case PrometheusMatcherType::NRE: result += "!~"; break;
        }
        result += quotePrometheusString(matcher.label_value);
        return result;
    }

    String formatPrometheusAggregationPrefix(const PrometheusQueryTree::AggregationOperator & aggregation)
    {
        String result = aggregation.operator_name;
        if (aggregation.without || (aggregation.by && !aggregation.labels.empty()))
        {
            result += aggregation.without ? " without (" : " by (";
            for (size_t i = 0; i != aggregation.labels.size(); ++i)
            {
                if (i != 0)
                    result += ", ";
                result += formatPrometheusLabelName(aggregation.labels[i]);
            }
            result += ") ";
        }
        return result;
    }

    bool hasPrometheusVectorMatching(const PrometheusQueryTree::BinaryOperator & binary)
    {
        /// Prometheus drops an empty `ignoring()` modifier when printing, but
        /// keeps `on()` because the latter changes the matching mode.
        return binary.on || !binary.labels.empty() || binary.group_left || binary.group_right;
    }

    constexpr size_t max_characters_per_line = 100;

    String formatPrometheusNodeFlat(const PrometheusQueryTree::Node & node, const PrometheusQueryTree & tree);

    String formatPrometheusNode(const PrometheusQueryTree::Node & node, const PrometheusQueryTree & tree, size_t level);

    String formatPrometheusNodeWithParentheses(const PrometheusQueryTree::Node & node, const PrometheusQueryTree & tree, size_t level);

    bool needsPrometheusLeftParentheses(const PrometheusQueryTree::BinaryOperator & binary)
    {
        const auto precedence = binary.getPrecedence();
        const auto left_precedence = binary.getLeftArgument()->getPrecedence();
        return (precedence < left_precedence) || (precedence == left_precedence && binary.isRightAssociative());
    }

    bool needsPrometheusRightParentheses(const PrometheusQueryTree::BinaryOperator & binary)
    {
        return binary.getPrecedence() <= binary.getRightArgument()->getPrecedence();
    }

    String formatPrometheusSubquerySuffix(const PrometheusQueryTree::Subquery & subquery, const PrometheusQueryTree & tree)
    {
        String result = fmt::format("[{}:", formatPrometheusDuration(subquery.range, tree.getTimestampScale()));
        if (subquery.step)
            result += formatPrometheusDuration(*subquery.step, tree.getTimestampScale());
        result += ']';
        return result;
    }

    String formatPrometheusNodeWithParentheses(const PrometheusQueryTree::Node & node, const PrometheusQueryTree & tree, size_t level)
    {
        const String flat = formatPrometheusNodeFlat(node, tree);
        const String indent(level * 2, ' ');
        if (flat.size() <= max_characters_per_line)
        {
            String result = indent + '(';
            result += flat;
            result += ')';
            return result;
        }

        return indent + "(\n" + formatPrometheusNode(node, tree, level + 1) + "\n" + indent + ')';
    }

    String formatPrometheusNodeFlat(const PrometheusQueryTree::Node & node, const PrometheusQueryTree & tree)
    {
        using NodeType = PrometheusQueryTree::NodeType;
        switch (node.node_type)
        {
            case NodeType::Scalar:
                return formatPrometheusScalar(static_cast<const PrometheusQueryTree::Scalar &>(node));

            case NodeType::StringLiteral:
                return quotePrometheusString(static_cast<const PrometheusQueryTree::StringLiteral &>(node).string);

            case NodeType::InstantSelector:
            {
                const auto & selector = static_cast<const PrometheusQueryTree::InstantSelector &>(node);
                size_t metric_name_matcher_count = 0;
                size_t metric_name_pos = static_cast<size_t>(-1);
                for (size_t i = 0; i != selector.matchers.size(); ++i)
                {
                    const auto & matcher = selector.matchers[i];
                    if (matcher.label_name == "__name__")
                    {
                        ++metric_name_matcher_count;
                        if (matcher.matcher_type == PrometheusMatcherType::EQ && metric_name_pos == static_cast<size_t>(-1))
                            metric_name_pos = i;
                    }
                }

                const bool can_hoist_metric_name
                    = !selector.metric_name.empty()
                    && metric_name_matcher_count == 1
                    && metric_name_pos != static_cast<size_t>(-1)
                    && selector.matchers[metric_name_pos].label_value == selector.metric_name
                    && canPrintMetricNameUnquoted(selector.metric_name);

                String result;
                if (can_hoist_metric_name)
                    result += formatPrometheusMetricName(selector.metric_name);

                std::vector<String> matcher_strings;
                matcher_strings.reserve(selector.matchers.size());
                for (size_t i = 0; i != selector.matchers.size(); ++i)
                {
                    if (i == metric_name_pos && can_hoist_metric_name)
                        continue;
                    matcher_strings.push_back(formatPrometheusMatcher(selector.matchers[i]));
                }
                std::sort(matcher_strings.begin(), matcher_strings.end());

                if (!matcher_strings.empty())
                {
                    result += "{";
                    for (size_t i = 0; i != matcher_strings.size(); ++i)
                    {
                        if (i != 0)
                            result += ',';
                        result += matcher_strings[i];
                    }
                    result += "}";
                }
                return result;
            }

            case NodeType::RangeSelector:
            {
                const auto & range = static_cast<const PrometheusQueryTree::RangeSelector &>(node);
                return fmt::format("{}[{}]", formatPrometheusNodeFlat(*range.getInstantSelector(), tree),
                                   formatPrometheusDuration(range.range, tree.getTimestampScale()));
            }

            case NodeType::Subquery:
            {
                const auto & subquery = static_cast<const PrometheusQueryTree::Subquery &>(node);
                const auto * expression = subquery.getExpression();
                const bool need_parentheses = subquery.getPrecedence() <= expression->getPrecedence();
                String result;
                if (need_parentheses)
                    result += '(';
                result += formatPrometheusNodeFlat(*expression, tree);
                if (need_parentheses)
                    result += ')';
                result += formatPrometheusSubquerySuffix(subquery, tree);
                return result;
            }

            case NodeType::Offset:
            {
                const auto & offset = static_cast<const PrometheusQueryTree::Offset &>(node);
                String result = formatPrometheusNodeFlat(*offset.getExpression(), tree);
                switch (offset.at_modifier)
                {
                    case PrometheusQueryTree::Offset::AtModifier::None:
                        break;
                    case PrometheusQueryTree::Offset::AtModifier::Timestamp:
                        chassert(offset.at_timestamp);
                        result += " @ ";
                        result += formatPrometheusTimestamp(*offset.at_timestamp, tree.getTimestampScale());
                        break;
                    case PrometheusQueryTree::Offset::AtModifier::Start:
                        result += " @ start()";
                        break;
                    case PrometheusQueryTree::Offset::AtModifier::End:
                        result += " @ end()";
                        break;
                }
                if (offset.offset_value)
                    result += fmt::format(" offset {}", formatPrometheusDuration(*offset.offset_value, tree.getTimestampScale()));
                return result;
            }

            case NodeType::Function:
            {
                const auto & function = static_cast<const PrometheusQueryTree::Function &>(node);
                String result = function.function_name + '(';
                for (size_t i = 0; i != function.getArguments().size(); ++i)
                {
                    if (i != 0)
                        result += ", ";
                    result += formatPrometheusNodeFlat(*function.getArguments()[i], tree);
                }
                result += ')';
                return result;
            }

            case NodeType::UnaryOperator:
            {
                const auto & unary = static_cast<const PrometheusQueryTree::UnaryOperator &>(node);
                if (const auto * scalar = typeid_cast<const PrometheusQueryTree::Scalar *>(unary.getArgument()); scalar && std::isinf(scalar->scalar))
                    return unary.operator_name + String(scalar->scalar < 0 ? "-Inf" : "Inf");

                const bool need_parentheses = unary.getPrecedence() < unary.getArgument()->getPrecedence();
                String result = unary.operator_name;
                if (need_parentheses)
                    result += '(';
                result += formatPrometheusNodeFlat(*unary.getArgument(), tree);
                if (need_parentheses)
                    result += ')';
                return result;
            }

            case NodeType::BinaryOperator:
            {
                const auto & binary = static_cast<const PrometheusQueryTree::BinaryOperator &>(node);
                const bool need_left_parentheses = needsPrometheusLeftParentheses(binary);
                const bool need_right_parentheses = needsPrometheusRightParentheses(binary);

                String result;
                if (need_left_parentheses)
                    result += '(';
                result += formatPrometheusNodeFlat(*binary.getLeftArgument(), tree);
                if (need_left_parentheses)
                    result += ')';
                result += " ";
                result += binary.operator_name;
                if (binary.bool_modifier)
                    result += " bool";
                if (hasPrometheusVectorMatching(binary))
                {
                    result += binary.on ? " on (" : " ignoring (";
                    for (size_t i = 0; i != binary.labels.size(); ++i)
                    {
                        if (i != 0)
                            result += ", ";
                        result += formatPrometheusLabelName(binary.labels[i]);
                    }
                    result += ')';
                }
                if (binary.group_left || binary.group_right)
                {
                    result += binary.group_left ? " group_left (" : " group_right (";
                    for (size_t i = 0; i != binary.extra_labels.size(); ++i)
                    {
                        if (i != 0)
                            result += ", ";
                        result += formatPrometheusLabelName(binary.extra_labels[i]);
                    }
                    result += ')';
                }
                result += " ";
                if (need_right_parentheses)
                    result += '(';
                result += formatPrometheusNodeFlat(*binary.getRightArgument(), tree);
                if (need_right_parentheses)
                    result += ')';
                return result;
            }

            case NodeType::AggregationOperator:
            {
                const auto & aggregation = static_cast<const PrometheusQueryTree::AggregationOperator &>(node);
                String result = formatPrometheusAggregationPrefix(aggregation) + '(';
                for (size_t i = 0; i != aggregation.getArguments().size(); ++i)
                {
                    if (i != 0)
                        result += ", ";
                    result += formatPrometheusNodeFlat(*aggregation.getArguments()[i], tree);
                }
                result += ')';
                return result;
            }
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Prometheus query node type");
    }

    String formatPrometheusNode(const PrometheusQueryTree::Node & node, const PrometheusQueryTree & tree, size_t level)
    {
        String flat = formatPrometheusNodeFlat(node, tree);
        const String indent(level * 2, ' ');

        switch (node.node_type)
        {
            case PrometheusQueryTree::NodeType::Function:
            {
                const auto & function = static_cast<const PrometheusQueryTree::Function &>(node);
                if (flat.size() <= max_characters_per_line)
                    return indent + flat;

                String result = indent + function.function_name + "(\n";
                for (size_t i = 0; i != function.getArguments().size(); ++i)
                {
                    if (i != 0)
                        result += ",\n";
                    result += formatPrometheusNode(*function.getArguments()[i], tree, level + 1);
                }
                result += "\n" + indent + ')';
                return result;
            }

            case PrometheusQueryTree::NodeType::AggregationOperator:
            {
                const auto & aggregation = static_cast<const PrometheusQueryTree::AggregationOperator &>(node);
                if (flat.size() <= max_characters_per_line)
                    return indent + flat;

                String result = indent + formatPrometheusAggregationPrefix(aggregation) + "(\n";
                for (size_t i = 0; i != aggregation.getArguments().size(); ++i)
                {
                    if (i != 0)
                        result += ",\n";
                    result += formatPrometheusNode(*aggregation.getArguments()[i], tree, level + 1);
                }
                result += "\n" + indent + ')';
                return result;
            }

            case PrometheusQueryTree::NodeType::BinaryOperator:
            {
                const auto & binary = static_cast<const PrometheusQueryTree::BinaryOperator &>(node);
                const bool need_left_parentheses = needsPrometheusLeftParentheses(binary);
                const bool need_right_parentheses = needsPrometheusRightParentheses(binary);
                if (flat.size() <= max_characters_per_line)
                    return indent + flat;

                String result = need_left_parentheses
                    ? formatPrometheusNodeWithParentheses(*binary.getLeftArgument(), tree, level + 1)
                    : formatPrometheusNode(*binary.getLeftArgument(), tree, level + 1);
                result += "\n" + indent + binary.operator_name;
                if (binary.bool_modifier)
                    result += " bool";
                if (hasPrometheusVectorMatching(binary))
                {
                    result += binary.on ? " on (" : " ignoring (";
                    for (size_t i = 0; i != binary.labels.size(); ++i)
                    {
                        if (i != 0)
                            result += ", ";
                        result += formatPrometheusLabelName(binary.labels[i]);
                    }
                    result += ')';
                }
                if (binary.group_left || binary.group_right)
                {
                    result += binary.group_left ? " group_left (" : " group_right (";
                    for (size_t i = 0; i != binary.extra_labels.size(); ++i)
                    {
                        if (i != 0)
                            result += ", ";
                        result += formatPrometheusLabelName(binary.extra_labels[i]);
                    }
                    result += ')';
                }
                result += "\n";
                result += need_right_parentheses
                    ? formatPrometheusNodeWithParentheses(*binary.getRightArgument(), tree, level + 1)
                    : formatPrometheusNode(*binary.getRightArgument(), tree, level + 1);
                return result;
            }

            case PrometheusQueryTree::NodeType::UnaryOperator:
            {
                const auto & unary = static_cast<const PrometheusQueryTree::UnaryOperator &>(node);
                if (flat.size() <= max_characters_per_line)
                    return indent + flat;

                const bool need_parentheses = unary.getPrecedence() < unary.getArgument()->getPrecedence();
                String child = need_parentheses
                    ? formatPrometheusNodeWithParentheses(*unary.getArgument(), tree, level)
                    : formatPrometheusNode(*unary.getArgument(), tree, level);
                const auto first_non_space = child.find_first_not_of(' ');
                if (first_non_space != String::npos)
                    child.erase(0, first_non_space);
                return indent + unary.operator_name + child;
            }

            case PrometheusQueryTree::NodeType::Subquery:
            {
                const auto & subquery = static_cast<const PrometheusQueryTree::Subquery &>(node);
                if (flat.size() <= max_characters_per_line)
                    return flat;

                const auto * expression = subquery.getExpression();
                const bool need_parentheses = subquery.getPrecedence() <= expression->getPrecedence();
                String result = need_parentheses
                    ? formatPrometheusNodeWithParentheses(*expression, tree, level)
                    : formatPrometheusNode(*expression, tree, level);
                result += formatPrometheusSubquerySuffix(subquery, tree);
                return result;
            }

            default:
                return indent + flat;
        }
    }

    template <typename NodeType>
    NodeType * cloneNodeImpl(const NodeType * node, std::vector<std::unique_ptr<Node>> & node_list)
    {
        auto new_node = std::make_unique<NodeType>(*node);
        auto * ptr = new_node.get();
        for (const auto * & child : new_node->children)
        {
            auto * new_child = child->clone(node_list);
            new_child->parent = new_node.get();
            child = new_child;
        }
        new_node->parent = nullptr;
        node_list.emplace_back(std::move(new_node));
        return ptr;
    }
}

Node * PrometheusQueryTree::Scalar::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::StringLiteral::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::InstantSelector::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::RangeSelector::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::Subquery::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::Offset::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::Function::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::UnaryOperator::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::BinaryOperator::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::AggregationOperator::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

PrometheusQueryTree & PrometheusQueryTree::operator=(const PrometheusQueryTree & src)
{
    if (this != &src)
    {
        const Node * new_root = nullptr;
        std::vector<std::unique_ptr<Node>> new_node_list;
        new_node_list.reserve(src.node_list.size());

        if (src.root)
            new_root = src.root->clone(new_node_list);

        *this = PrometheusQueryTree{std::move(new_node_list), new_root, src.timestamp_scale};
    }
    return *this;
}

PrometheusQueryTree::PrometheusQueryTree(std::vector<std::unique_ptr<Node>> node_list_, const Node * root_, UInt32 timestamp_scale_)
    : node_list(std::move(node_list_))
    , root(root_)
    , timestamp_scale(timestamp_scale_)
{
}

PrometheusQueryTree::PrometheusQueryTree(std::unique_ptr<Node> single_node_, UInt32 timestamp_scale_)
    : timestamp_scale(timestamp_scale_)
{
    node_list.emplace_back(std::move(single_node_));
    root = node_list.back().get();
}

PrometheusQueryTree & PrometheusQueryTree::operator=(PrometheusQueryTree && src) noexcept
{
    node_list = std::exchange(src.node_list, {});
    root = std::exchange(src.root, nullptr);
    timestamp_scale = std::exchange(src.timestamp_scale, 0);
    return *this;
}

PrometheusQueryResultType PrometheusQueryTree::getResultType() const
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query tree shouldn't be empty");
    return root->result_type;
}

namespace
{
    constexpr const size_t NUM_SPACES_PER_INDENT = 4;

    String makeIndent(size_t indent) { return String(indent * NUM_SPACES_PER_INDENT, ' '); }
}

String PrometheusQueryTree::dumpTree() const
{
    if (root)
        return fmt::format("\nPrometheusQueryTree({}):\n{}\n", root->result_type, root->dumpNode(*this, 1));
    else
        return "\nPrometheusQueryTree(EMPTY)\n";
}

String PrometheusQueryTree::Scalar::dumpNode(const PrometheusQueryTree & /* tree */, size_t indent) const
{
    return fmt::format("{}Scalar({})", makeIndent(indent), ::DB::toString(scalar));
}

String PrometheusQueryTree::StringLiteral::dumpNode(const PrometheusQueryTree & /* tree */, size_t indent) const
{
    return fmt::format("{}StringLiteral({})", makeIndent(indent), quoteString(string));
}

String PrometheusQueryTree::InstantSelector::dumpNode(const PrometheusQueryTree & /* tree */, size_t indent) const
{
    String str = fmt::format("{}InstantSelector:", makeIndent(indent));
    for (const auto & matcher : matchers)
        str += fmt::format("\n{}{} {} {}", makeIndent(indent + 1), matcher.label_name, matcher.matcher_type, quoteString(matcher.label_value));
    return str;
}

String PrometheusQueryTree::RangeSelector::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}RangeSelector:", makeIndent(indent));
    str += fmt::format("\n{}range: {}", makeIndent(indent + 1), ::DB::toString(range, tree.timestamp_scale));
    str += fmt::format("\n{}", getInstantSelector()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::Subquery::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}Subquery:", makeIndent(indent));
    str += fmt::format("\n{}range: {}", makeIndent(indent + 1), ::DB::toString(range, tree.timestamp_scale));
    if (step)
        str += fmt::format("\n{}step: {}", makeIndent(indent + 1), ::DB::toString(*step, tree.timestamp_scale));
    str += fmt::format("\n{}", getExpression()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::Offset::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}Offset:", makeIndent(indent));
    switch (at_modifier)
    {
        case AtModifier::None:
            break;
        case AtModifier::Timestamp:
            chassert(at_timestamp);
            str += fmt::format("\n{}at: {}", makeIndent(indent + 1), ::DB::toString(*at_timestamp, tree.timestamp_scale));
            break;
        case AtModifier::Start:
            str += fmt::format("\n{}at: start()", makeIndent(indent + 1));
            break;
        case AtModifier::End:
            str += fmt::format("\n{}at: end()", makeIndent(indent + 1));
            break;
    }
    if (offset_value)
        str += fmt::format("\n{}offset: {}", makeIndent(indent + 1), ::DB::toString(*offset_value, tree.timestamp_scale));
    str += fmt::format("\n{}", getExpression()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::Function::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    const auto & arguments = getArguments();
    std::string_view maybe_colon = arguments.empty() ? "" : ":";
    String str = fmt::format("{}Function({}){}", makeIndent(indent), function_name, maybe_colon);
    for (const auto * argument : arguments)
        str += fmt::format("\n{}", argument->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::UnaryOperator::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}UnaryOperator({})", makeIndent(indent), operator_name);
    str += fmt::format("\n{}", getArgument()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::BinaryOperator::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}BinaryOperator({})", makeIndent(indent), operator_name);
    if (bool_modifier)
        str += fmt::format("\n{}bool", makeIndent(indent + 1));
    if (on || ignoring)
    {
        std::string_view on_or_ignoring = on ? "on" : "ignoring";
        String joined_labels;
        if (!labels.empty())
            joined_labels += fmt::format(" {}", fmt::join(labels, ", "));
        str += fmt::format("\n{}{}{}", makeIndent(indent + 1), on_or_ignoring, joined_labels);
    }
    if (group_left || group_right)
    {
        std::string_view group_left_or_right = group_left ? "group_left" : "group_right";
        String joined_extra_labels;
        if (!extra_labels.empty())
            joined_extra_labels += fmt::format(" {}", fmt::join(extra_labels, ", "));
        str += fmt::format("\n{}{}{}", makeIndent(indent + 1), group_left_or_right, joined_extra_labels);
    }
    str += fmt::format("\n{}", getLeftArgument()->dumpNode(tree, indent + 1));
    str += fmt::format("\n{}", getRightArgument()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::AggregationOperator::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}AggregationOperator({})", makeIndent(indent), operator_name);
    if (by || without)
    {
        std::string_view by_or_without = by ? "by" : "without";
        String joined_labels;
        if (!labels.empty())
            joined_labels += fmt::format(" {}", fmt::join(labels, ", "));
        str += fmt::format("\n{}{}{}", makeIndent(indent + 1), by_or_without, joined_labels);
    }
    for (const auto * argument : getArguments())
        str += fmt::format("\n{}", argument->dumpNode(tree, indent + 1));
    return str;
}


void PrometheusQueryTree::parse(std::string_view promql_query_, UInt32 timestamp_scale_)
{
    String error_message;
    size_t error_pos = 0;
    if (PrometheusQueryParsingUtil::tryParseQuery(promql_query_, timestamp_scale_, *this, &error_message, &error_pos))
        return;

    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "{} at position {} while parsing PromQL query: {}",
                    error_message, error_pos, promql_query_);
}

bool PrometheusQueryTree::tryParse(std::string_view promql_query_, UInt32 timestamp_scale_, String * error_message_, size_t * error_pos_)
{
    return PrometheusQueryParsingUtil::tryParseQuery(promql_query_, timestamp_scale_, *this, error_message_, error_pos_);
}

void PrometheusQueryTree::validate() const
{
    if (root)
        validateNode(root);
}


String PrometheusQueryTree::Scalar::toString(const PrometheusQueryTree &) const
{
    if (std::isfinite(scalar))
    {
        return ::DB::toString(scalar);
    }
    else if (std::isinf(scalar))
    {
        String str;
        if (scalar < 0)
            str += "-";
        str += "Inf";
        return str;
    }
    else
    {
        return "NaN";
    }
}

String PrometheusQueryTree::StringLiteral::toString(const PrometheusQueryTree &) const
{
    return quotePromQLString(string);
}

String PrometheusQueryTree::InstantSelector::toString(const PrometheusQueryTree &) const
{
    size_t metric_name_matcher_count = 0;
    size_t metric_name_pos = static_cast<size_t>(-1);
    for (size_t i = 0; i != matchers.size(); ++i)
    {
        const auto & matcher = matchers[i];
        if (matcher.label_name == "__name__")
        {
            ++metric_name_matcher_count;
            if (matcher.matcher_type == MatcherType::EQ && metric_name_pos == static_cast<size_t>(-1))
                metric_name_pos = i;
        }
    }

    const bool can_hoist_metric_name
        = metric_name_matcher_count == 1
        && metric_name_pos != static_cast<size_t>(-1)
        && canPrintMetricNameUnquoted(matchers[metric_name_pos].label_value);

    String str;
    if (can_hoist_metric_name)
        str += formatMetricName(matchers[metric_name_pos].label_value);

    if (!can_hoist_metric_name || matchers.size() > 1)
    {
        str += "{";
        bool need_comma = false;
        for (size_t i = 0; i != matchers.size(); ++i)
        {
            if (i == metric_name_pos && can_hoist_metric_name)
                continue;
            const auto & matcher = matchers[i];
            if (need_comma)
                str += ",";

            const bool is_quoted_metric_name
                = matcher.label_name == "__name__"
                && matcher.matcher_type == MatcherType::EQ
                && !matcher.label_value.empty();
            if (is_quoted_metric_name)
            {
                str += quotePromQLString(matcher.label_value);
            }
            else
            {
                str += formatLabelName(matcher.label_name);
            }
            std::string_view matcher_type_str;
            switch (matcher.matcher_type)
            {
                case MatcherType::EQ:  matcher_type_str = "=";  break;
                case MatcherType::NE:  matcher_type_str = "!="; break;
                case MatcherType::RE:  matcher_type_str = "=~"; break;
                case MatcherType::NRE: matcher_type_str = "!~"; break;
            }
            chassert(!matcher_type_str.empty());
            if (!is_quoted_metric_name)
            {
                str += matcher_type_str;
                str += quotePromQLString(matcher.label_value);
            }
            need_comma = true;
        }
        str += "}";
    }

    return str;
}

String PrometheusQueryTree::RangeSelector::toString(const PrometheusQueryTree & tree) const
{
    String str = getInstantSelector()->toString(tree);
    str += "[";
    str += DB::toString(range, tree.timestamp_scale);
    str += "]";
    return str;
}

String PrometheusQueryTree::Subquery::toString(const PrometheusQueryTree & tree) const
{
    bool need_parentheses = (getPrecedence() <= getExpression()->getPrecedence());

    String str;
    if (need_parentheses)
        str += "(";
    str += getExpression()->toString(tree);
    if (need_parentheses)
        str += ")";

    str += "[";
    str += DB::toString(range, tree.timestamp_scale);
    str += ":";
    if (step)
        str += DB::toString(*step, tree.timestamp_scale);
    str += "]";

    return str;
}

String PrometheusQueryTree::Offset::toString(const PrometheusQueryTree & tree) const
{
    String str = getExpression()->toString(tree);
    switch (at_modifier)
    {
        case AtModifier::None:
            break;
        case AtModifier::Timestamp:
            chassert(at_timestamp);
            str += " @ ";
            str += DB::toString(Decimal64{*at_timestamp}, tree.timestamp_scale);
            break;
        case AtModifier::Start:
            str += " @ start()";
            break;
        case AtModifier::End:
            str += " @ end()";
            break;
    }
    if (offset_value)
    {
        str += " offset ";
        str += DB::toString(Decimal64{*offset_value}, tree.timestamp_scale);
    }
    return str;
}

String PrometheusQueryTree::Function::toString(const PrometheusQueryTree & tree) const
{
    String str = function_name;
    str += "(";
    bool need_comma = false;
    for (const auto * arg : getArguments())
    {
        if (need_comma)
            str += ", ";
        str += arg->toString(tree);
        need_comma = true;
    }
    str += ")";
    return str;
}

String PrometheusQueryTree::UnaryOperator::toString(const PrometheusQueryTree & tree) const
{
    bool need_parentheses = (getPrecedence() < getArgument()->getPrecedence());
    String str = operator_name;
    if (need_parentheses)
        str += "(";
    str += getArgument()->toString(tree);
    if (need_parentheses)
        str += ")";
    return str;
}

String PrometheusQueryTree::BinaryOperator::toString(const PrometheusQueryTree & tree) const
{
    auto precedence = getPrecedence();
    auto left_arg_precedence = getLeftArgument()->getPrecedence();
    auto right_arg_precedence = getRightArgument()->getPrecedence();
    bool need_left_parentheses = (precedence < left_arg_precedence) || (precedence == left_arg_precedence && isRightAssociative());
    bool need_right_parentheses = (precedence <= right_arg_precedence);

    String str;
    if (need_left_parentheses)
        str += "(";
    str += getLeftArgument()->toString(tree);
    if (need_left_parentheses)
        str += ")";

    str += " ";
    str += operator_name;
    str += " ";

    if (bool_modifier)
        str += "bool ";

    if (on)
        str += "on(";
    else if (ignoring)
        str += "ignoring(";

    if (on || ignoring)
    {
        bool need_comma = false;
        for (const auto & label : labels)
        {
            if (need_comma)
                str += ", ";
            str += formatLabelName(label);
            need_comma = true;
        }
        str += ") ";
    }

    if (group_left)
        str += "group_left";
    else if (group_right)
        str += "group_right";

    if (group_left || group_right)
    {
        if (!extra_labels.empty())
        {
            str += "(";
            bool need_comma = false;
            for (const auto & label : extra_labels)
            {
                if (need_comma)
                    str += ", ";
                str += formatLabelName(label);
                need_comma = true;
            }
            str += ")";
        }
        str += " ";
    }

    if (need_right_parentheses)
        str += "(";
    str += getRightArgument()->toString(tree);
    if (need_right_parentheses)
        str += ")";

    return str;
}

int PrometheusQueryTree::Scalar::getPrecedence() const
{
    if ((std::isfinite(scalar) || std::isinf(scalar)) && scalar < 0)
        return 3; /// same as unary operator '-'
    else
        return 0;
}

int PrometheusQueryTree::Subquery::getPrecedence() const
{
    return 1; /// before anything what have precedence (we need parentheses around `expr` in "(expr)[1d:1h]" if expr is any operator)
}

int PrometheusQueryTree::UnaryOperator::getPrecedence() const
{
    return 3; /// same as binary operator '*'
}

int PrometheusQueryTree::BinaryOperator::getPrecedence() const
{
    if (operator_name == "^")
        return 2;
    if ((operator_name == "*") || (operator_name == "/") || (operator_name == "%") || (operator_name == "atan2"))
        return 3;
    if ((operator_name == "+") || (operator_name == "-"))
        return 4;
    if ((operator_name == "==") || (operator_name == "!=") || (operator_name == "<") || (operator_name == ">") || (operator_name == "<=") || (operator_name == ">="))
        return 5;
    if ((operator_name == "and") || (operator_name == "unless"))
        return 6;
    if (operator_name == "or")
        return 7;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", operator_name);
}

bool PrometheusQueryTree::BinaryOperator::isRightAssociative() const
{
    return (operator_name == "^"); /// 2 ^ 3 ^ 2 is equivalent to 2 ^ (3 ^ 2)
}

String PrometheusQueryTree::AggregationOperator::toString(const PrometheusQueryTree & tree) const
{
    String str = operator_name;

    if (by)
        str += " by (";
    else if (without)
        str += " without (";

    if (by || without)
    {
        bool need_comma = false;
        for (const auto & label : labels)
        {
            if (need_comma)
                str += ", ";
            str += formatLabelName(label);
            need_comma = true;
        }
        str += ") ";
    }

    str += "(";
    bool need_comma = false;
    for (const auto * arg : getArguments())
    {
        if (need_comma)
            str += ", ";
        str += arg->toString(tree);
        need_comma = true;
    }
    str += ")";

    return str;
}

String PrometheusQueryTree::toString() const
{
    if (empty())
        return "";
    return getRoot()->toString(*this);
}

String PrometheusQueryTree::toPrometheusString() const
{
    if (empty())
        return "";
    return formatPrometheusNode(*getRoot(), *this, 0);
}

}
