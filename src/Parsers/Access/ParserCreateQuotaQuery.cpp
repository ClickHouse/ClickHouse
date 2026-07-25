#include <IO/ReadHelpers.h>
#include <Access/IAccessStorage.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserCreateQuotaQuery.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseIntervalKind.h>
#include <base/range.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/StringUtils.h>
#include <base/hex.h>

#include <bit>
#include <cmath>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}


namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::RENAME_TO}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseKeyType(IParserBase::Pos & pos, Expected & expected, QuotaKeyType & key_type)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{Keyword::NOT_KEYED}.ignore(pos, expected))
            {
                key_type = QuotaKeyType::NONE;
                return true;
            }

            if (!ParserKeyword{Keyword::KEY_BY}.ignore(pos, expected) && !ParserKeyword{Keyword::KEYED_BY}.ignore(pos, expected))
                return false;

            Strings names;
            if (!parseIdentifiersOrStringLiterals(pos, expected, names))
                return false;

            String name = boost::algorithm::join(names, "_or_");
            boost::to_lower(name);
            boost::replace_all(name, " ", "_");

            for (auto kt : collections::range(QuotaKeyType::MAX))
            {
                if (QuotaKeyTypeInfo::get(kt).name == name)
                {
                    key_type = kt;
                    return true;
                }
            }

            String all_types_str;
            for (auto kt : collections::range(QuotaKeyType::MAX))
                all_types_str += String(all_types_str.empty() ? "" : ", ") + "'" + QuotaKeyTypeInfo::get(kt).name + "'";
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Quota cannot be keyed by '{}'. Expected one of the following identifiers: {}", name, all_types_str);
        });
    }

    bool parseIpPrefixBits(IParserBase::Pos & pos, Expected & expected,
                           std::optional<MaskBits> & ipv4_bits, std::optional<MaskBits> & ipv6_bits)
    {
        auto try_parse_prefix = [&](Keyword keyword, std::optional<MaskBits> & prefix_bits, UInt8 max_bits)
        {
            return IParserBase::wrapParseImpl(pos, [&]
            {
                if (!ParserKeyword{keyword}.ignore(pos, expected))
                    return false;

                ASTPtr value_ast;
                if (!ParserUnsignedInteger{}.parse(pos, value_ast, expected))
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected integer prefix length for IP address masking");

                UInt64 prefix = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value_ast->as<ASTLiteral &>().value);

                if (prefix > max_bits)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "{} prefix must be between 0 and {}",
                        toStringView(keyword),
                        static_cast<unsigned>(max_bits));

                prefix_bits = static_cast<MaskBits>(prefix);

                return true;
            });
        };
        bool parsed_any = false;
        parsed_any |= try_parse_prefix(Keyword::IPV4_PREFIX_BITS, ipv4_bits, 32);
        parsed_any |= try_parse_prefix(Keyword::IPV6_PREFIX_BITS, ipv6_bits, 128);
        return parsed_any;
    }

    bool parseQuotaType(IParserBase::Pos & pos, Expected & expected, QuotaType & quota_type)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            for (auto qt : collections::range(QuotaType::MAX))
            {
                if (ParserKeyword::createDeprecated(QuotaTypeInfo::get(qt).keyword).ignore(pos, expected))
                {
                    quota_type = qt;
                    return true;
                }
            }

            ASTPtr ast;
            if (!ParserIdentifier{}.parse(pos, ast, expected))
                return false;

            String name = getIdentifierName(ast);
            for (auto qt : collections::range(QuotaType::MAX))
            {
                if (QuotaTypeInfo::get(qt).name == name)
                {
                    quota_type = qt;
                    return true;
                }
            }

            return false;
        });
    }

    template <typename T>
    requires std::same_as<T, double> || std::same_as<T, QuotaValue>
    T fieldToNumber(const Field & f)
    {
        if (f.getType() == Field::Types::String)
            /// Parse with overflow checking so that a quoted out-of-range value (e.g. MAX queries = '1e20' or
            /// '18446744073709551616') throws instead of silently wrapping around before the range checks below.
            return static_cast<T>(parseWithSizeSuffix<QuotaValue, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(boost::algorithm::trim_copy(f.safeGet<std::string>())));
        return applyVisitor(FieldVisitorConvertToNumber<T>(), f);
    }

    /// Whether a numeric literal denotes an integral value, determined from the literal text itself.
    /// The Float64 field produced by ParserNumber has already been rounded to the nearest double,
    /// so for a value above 2^53 a fractional part can vanish before any check on the value:
    /// e.g. the literal 9007199254740992.5 is rounded to 9007199254740992.0. Handles a decimal
    /// literal with an optional exponent (1.5, 1.5e1) and a hexadecimal float (0x1.8p1). Text that
    /// does not match these forms (e.g. inf, nan) is reported as integral, leaving the rejection
    /// to the range check inside FieldVisitorConvertToNumber.
    bool isIntegralNumericLiteral(std::string_view text)
    {
        if (!text.empty() && (text.front() == '+' || text.front() == '-'))
            text.remove_prefix(1);

        bool is_hex = text.starts_with("0x") || text.starts_with("0X");
        if (is_hex)
            text.remove_prefix(2);

        auto is_digit = [is_hex](char c) { return is_hex ? isHexDigit(c) : isNumericASCII(c); };

        size_t i = 0;
        while (i < text.size() && is_digit(text[i]))
            ++i;
        std::string_view integer_part = text.substr(0, i);

        std::string_view fractional_part;
        if (i < text.size() && text[i] == '.')
        {
            size_t fraction_begin = ++i;
            while (i < text.size() && is_digit(text[i]))
                ++i;
            fractional_part = text.substr(fraction_begin, i - fraction_begin);
        }

        /// The exponent shifts the point by decimal digits for a decimal literal ('e')
        /// and by bits for a hexadecimal one ('p'); its own digits are decimal in both cases.
        Int64 exponent = 0;
        char exponent_char = is_hex ? 'p' : 'e';
        if (i < text.size() && (text[i] | 0x20) == exponent_char)
        {
            ++i;
            bool exponent_negative = false;
            if (i < text.size() && (text[i] == '+' || text[i] == '-'))
            {
                exponent_negative = (text[i] == '-');
                ++i;
            }
            while (i < text.size() && isNumericASCII(text[i]))
            {
                /// Clamp: the point cannot usefully shift by more than the mantissa length anyway.
                exponent = std::min<Int64>(exponent * 10 + (text[i] - '0'), 1000000000);
                ++i;
            }
            if (exponent_negative)
                exponent = -exponent;
        }

        if (i != text.size() || (integer_part.empty() && fractional_part.empty()))
            return true; /// Not a numeric literal form we understand: leave it to the value checks.

        /// Find the deepest nonzero digit relative to the point: fractional digits have depths
        /// 1, 2, ..., integer digits have depths 0, -1, ... counting leftwards from the lowest one.
        Int64 deepest_position = 0;
        char deepest_digit = 0;
        for (size_t k = 0; k < fractional_part.size(); ++k)
        {
            if (fractional_part[k] != '0')
            {
                deepest_position = static_cast<Int64>(k) + 1;
                deepest_digit = fractional_part[k];
            }
        }
        if (deepest_digit == 0)
        {
            for (size_t k = 0; k < integer_part.size(); ++k)
            {
                if (integer_part[k] != '0')
                {
                    deepest_position = static_cast<Int64>(k) + 1 - static_cast<Int64>(integer_part.size());
                    deepest_digit = integer_part[k];
                }
            }
        }
        if (deepest_digit == 0)
            return true; /// All digits are zero: the value is 0.

        if (is_hex)
        {
            /// A hexadecimal digit at depth d occupies the bits at depths 4d-3 .. 4d after the
            /// binary point; the deepest set bit of the digit decides integrality.
            Int64 deepest_bit = 4 * deepest_position - std::countr_zero(static_cast<unsigned>(unhex(deepest_digit)));
            return deepest_bit <= exponent;
        }
        return deepest_position <= exponent;
    }

    bool parseMaxValue(IParserBase::Pos & pos, Expected & expected, QuotaType quota_type, QuotaValue & max_value)
    {
        IParserBase::Pos literal_pos = pos;
        ASTPtr ast;
        if (!ParserNumber{}.parse(pos, ast, expected) && !ParserStringLiteral{}.parse(pos, ast, expected))
            return false;

        /// ParserNumber consumes an optional sign token before the number token.
        if (literal_pos->type == TokenType::Minus || literal_pos->type == TokenType::Plus)
            ++literal_pos;
        /// ParserNumber strips '_' digit separators from the token before conversion; do the same
        /// so that e.g. 1_5e-1 is analyzed as 15e-1 and not dismissed as an unknown literal form.
        std::string literal_text(literal_pos->begin, literal_pos->size());
        std::erase(literal_text, '_');

        const Field & max_field = ast->as<ASTLiteral &>().value;
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        if (type_info.output_denominator == 1)
        {
            /// Reject negative literals explicitly: FieldVisitorConvertToNumber wraps a negative signed
            /// integer around instead of throwing (e.g. MAX queries = -1 would become 18446744073709551615).
            /// A negative float is checked by the sign bit rather than a `< 0` comparison: a tiny negative
            /// literal (e.g. MAX queries = -1e-400) underflows to -0.0, which compares equal to zero.
            bool is_negative = (max_field.getType() == Field::Types::Int64 && max_field.safeGet<Int64>() < 0)
                || (max_field.getType() == Field::Types::Int128 && max_field.safeGet<Int128>() < 0)
                || (max_field.getType() == Field::Types::Int256 && max_field.safeGet<Int256>() < 0)
                || (max_field.getType() == Field::Types::Float64 && std::signbit(max_field.safeGet<Float64>()));
            if (is_negative)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quota max value is out of range");
            /// Reject a fractional literal for an integer quota type: FieldVisitorConvertToNumber would
            /// silently truncate it (e.g. MAX queries = 1.5 would become 1 and round-trip differently),
            /// while the users.xml path rejects the same input. The check looks at the literal text
            /// because the Float64 field has already been rounded to the nearest double, which loses
            /// the fractional part of a value above 2^53 (e.g. 9007199254740992.5) before any check
            /// on the value itself. A non-finite value is reported as integral and rejected by the
            /// range check inside FieldVisitorConvertToNumber.
            if (max_field.getType() == Field::Types::Float64 && !isIntegralNumericLiteral(literal_text))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quota max value must be an integer");
            max_value = fieldToNumber<QuotaValue>(max_field);
        }
        else
        {
            /// Reject a negative value by the sign bit before scaling: a tiny negative literal
            /// (e.g. MAX execution_time = -1e-400) underflows to -0.0, which compares equal to zero.
            double value = fieldToNumber<double>(max_field);
            if (std::signbit(value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quota max value is out of range");
            /// Bound the scaled value to the QuotaValue (UInt64) range before the cast: an out-of-range or
            /// non-finite product (e.g. MAX execution_time = 1e19) makes static_cast<QuotaValue> undefined behavior.
            double scaled_value = value * static_cast<double>(type_info.output_denominator);
            static constexpr double uint64_max_plus_one_as_double = 18446744073709551616.0; /// 2^64, first double above UInt64 max
            if (!std::isfinite(scaled_value) || scaled_value >= uint64_max_plus_one_as_double)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quota max value is out of range");
            max_value = static_cast<QuotaValue>(scaled_value);
        }
        return true;
    }

    bool parseLimits(IParserBase::Pos & pos, Expected & expected, std::vector<std::pair<QuotaType, QuotaValue>> & limits)
    {
        std::vector<std::pair<QuotaType, QuotaValue>> res_limits;
        bool max_prefix_encountered = false;

        auto parse_limit = [&]
        {
            max_prefix_encountered |= ParserKeyword{Keyword::MAX}.ignore(pos, expected);

            QuotaType quota_type = {};
            if (!parseQuotaType(pos, expected, quota_type))
                return false;

            if (max_prefix_encountered)
            {
                ParserToken{TokenType::Equals}.ignore(pos, expected);
            }
            else
            {
                if (!ParserKeyword{Keyword::MAX}.ignore(pos, expected))
                    return false;
            }

            QuotaValue max_value = 0;
            if (!parseMaxValue(pos, expected, quota_type, max_value))
                return false;

            res_limits.emplace_back(quota_type, max_value);
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_limit, false))
            return false;

        limits = std::move(res_limits);
        return true;
    }

    bool parseIntervalsWithLimits(IParserBase::Pos & pos, Expected & expected, std::vector<ASTCreateQuotaQuery::Limits> & all_limits)
    {
        std::vector<ASTCreateQuotaQuery::Limits> res_all_limits;

        auto parse_interval_with_limits = [&]
        {
            if (!ParserKeyword{Keyword::FOR}.ignore(pos, expected))
                return false;

            ASTCreateQuotaQuery::Limits limits;
            limits.randomize_interval = ParserKeyword{Keyword::RANDOMIZED}.ignore(pos, expected);

            ParserKeyword{Keyword::INTERVAL}.ignore(pos, expected);

            ASTPtr num_intervals_ast;
            if (!ParserNumber{}.parse(pos, num_intervals_ast, expected))
                return false;

            double num_intervals = applyVisitor(FieldVisitorConvertToNumber<double>(), num_intervals_ast->as<ASTLiteral &>().value);

            IntervalKind interval_kind;
            if (!parseIntervalKind(pos, expected, interval_kind))
                return false;

            /// Bound the seconds to the finite Int64 range before the cast: an out-of-range or non-finite
            /// double (e.g. FOR INTERVAL 1e19 SECOND) makes static_cast<Int64> undefined behavior.
            double total_seconds = num_intervals * interval_kind.toAvgSeconds();
            static constexpr double int64_max_as_double = 9223372036854775808.0; /// 2^63, first double above Int64 max
            if (!std::isfinite(total_seconds) || total_seconds >= int64_max_as_double || total_seconds < -int64_max_as_double)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quota interval duration is out of range");
            limits.duration = std::chrono::seconds(static_cast<Int64>(total_seconds));
            std::vector<std::pair<QuotaType, QuotaValue>> new_limits;

            if (ParserKeyword{Keyword::NO_LIMITS}.ignore(pos, expected))
            {
                limits.drop = true;
            }
            else if (ParserKeyword{Keyword::TRACKING_ONLY}.ignore(pos, expected))
            {
            }
            else if (parseLimits(pos, expected, new_limits))
            {
                for (const auto & [quota_type, max_value] : new_limits)
                    limits.max[static_cast<size_t>(quota_type)] = max_value;
            }
            else
                return false;

            res_all_limits.emplace_back(std::move(limits));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_interval_with_limits, false))
            return false;

        all_limits = std::move(res_all_limits);
        return true;
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr node;
            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoles().allowUsers().allowCurrentUser().useIDMode(id_mode);
            if (!ParserKeyword{Keyword::TO}.ignore(pos, expected) || !roles_p.parse(pos, node, expected))
                return false;

            roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(node);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateQuotaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_QUOTA}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_QUOTA}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_QUOTA}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{Keyword::IF_NOT_EXISTS}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{Keyword::OR_REPLACE}.ignore(pos, expected))
            or_replace = true;
    }

    Strings names;
    if (!parseIdentifiersOrStringLiterals(pos, expected, names))
        return false;

    String new_name;
    std::optional<QuotaKeyType> key_type;
    std::optional<MaskBits> ipv4_prefix_bits;
    std::optional<MaskBits> ipv6_prefix_bits;
    std::vector<ASTCreateQuotaQuery::Limits> all_limits;
    String cluster;
    String storage_name;

    while (true)
    {
        if (alter && new_name.empty() && (names.size() == 1) && parseRenameTo(pos, expected, new_name))
            continue;

        if (!key_type)
        {
            QuotaKeyType new_key_type = {};
            if (parseKeyType(pos, expected, new_key_type))
            {
                key_type = new_key_type;
                if (new_key_type == QuotaKeyType::IP_ADDRESS || new_key_type == QuotaKeyType::FORWARDED_IP_ADDRESS)
                    parseIpPrefixBits(pos, expected, ipv4_prefix_bits, ipv6_prefix_bits);
                continue;
            }
        }

        if (!ipv4_prefix_bits || !ipv6_prefix_bits)
        {
            if (parseIpPrefixBits(pos, expected, ipv4_prefix_bits, ipv6_prefix_bits))
                continue;
        }

        if (parseIntervalsWithLimits(pos, expected, all_limits))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        break;
    }

    boost::intrusive_ptr<ASTRolesOrUsersSet> roles;
    parseToRoles(pos, expected, attach_mode, roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    /// Validate that prefix bits are only used with IP_ADDRESS or FORWARDED_IP_ADDRESS key type
    if ((ipv4_prefix_bits || ipv6_prefix_bits) && key_type
        && *key_type != QuotaKeyType::IP_ADDRESS && *key_type != QuotaKeyType::FORWARDED_IP_ADDRESS)
    {
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "IP prefix bits can only be specified for quotas KEYED BY ip_address or forwarded_ip_address");
    }

    auto query = make_intrusive<ASTCreateQuotaQuery>();
    node = query;

    query->alter = alter;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_name = std::move(new_name);
    query->key_type = key_type;
    query->ipv4_prefix_bits = ipv4_prefix_bits;
    query->ipv6_prefix_bits = ipv6_prefix_bits;
    query->all_limits = std::move(all_limits);
    query->roles = std::move(roles);
    query->storage_name = std::move(storage_name);

    return true;
}
}
