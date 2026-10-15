#include <Common/StringUtils.h>
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
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <base/insertAtEnd.h>
#include <base/range.h>
#include <Common/FieldVisitorConvertToNumber.h>


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

            String name;
            for (const auto & part : names)
            {
                if (!name.empty())
                    name += "_or_";
                name += part;
            }
            toLowerASCII(name);
            std::replace(name.begin(), name.end(), ' ', '_');

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
            return static_cast<T>(parseWithSizeSuffix<QuotaValue>(trim(f.safeGet<std::string>(), isWhitespaceASCII)));
        return applyVisitor(FieldVisitorConvertToNumber<T>(), f);
    }

    bool parseMaxValue(IParserBase::Pos & pos, Expected & expected, QuotaType quota_type, QuotaValue & max_value)
    {
        ASTPtr ast;
        if (!ParserNumber{}.parse(pos, ast, expected) && !ParserStringLiteral{}.parse(pos, ast, expected))
            return false;

        const Field & max_field = ast->as<ASTLiteral &>().value;
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        if (type_info.output_denominator == 1)
            max_value = fieldToNumber<QuotaValue>(max_field);
        else
            max_value = type_info.scaleToValue(fieldToNumber<double>(max_field));
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

        insertAtEnd(all_limits, std::move(res_all_limits));
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

namespace DB
{

void registerStatementQuota(StatementFactory & factory)
{
    factory.registerStatement("CREATE QUOTA",
    {
        .description = R"DOCS_MD(
Creates a [quota](/concepts/features/security/access-rights#quotas-management) that can be assigned to a user or a role.

Syntax:

```sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | written_bytes | execution_time | failed_sequential_authentications | queries_per_normalized_hash} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

Keys `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address`, and `normalized_query_hash` correspond to the fields in the [system.quotas](/reference/system-tables/quotas) table.

`IPV4_PREFIX_BITS` and `IPV6_PREFIX_BITS` options can only be used when `KEYED BY` is `ip_address` or `forwarded_ip_address`. They correspond to the field in the [system.quotas](/reference/system-tables/quotas) table.

Parameters `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `written_bytes`, `execution_time`, `failed_sequential_authentications`, `queries_per_normalized_hash` correspond to the fields in the [system.quotas_usage](/reference/system-tables/quotas_usage) table.

`ON CLUSTER` clause allows creating quotas on a cluster, see [Distributed DDL](/reference/statements/distributed-ddl).

`CREATE QUOTA` requires the [CREATE QUOTA](/reference/statements/grant#access-management) privilege. `OR REPLACE` throws away an existing quota of the same name, including which roles it applies to, so it additionally requires the [DROP QUOTA](/reference/statements/grant#access-management) privilege. The `DROP QUOTA` privilege is required whether or not the quota already exists, so the statement cannot be used to find out which quotas exist.

**Examples**

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

For the default user limit the maximum execution time with half a second in 30 minutes, and limit the maximum number of queries with 321 and the maximum number of errors with 10 in 5 quarters:

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

Create a quota where each distinct normalized query pattern gets its own bucket, limited to 100 executions per hour:

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

Limit any single normalized query pattern to at most 50 executions per hour (regardless of the quota key type):

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

Further examples, using the xml configuration (not supported in ClickHouse Cloud), can be found in the [Quotas guide](/concepts/features/configuration/server-config/quotas).

## Related Content {#related-content}

- Blog: [Building single page applications with ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)
)DOCS_MD",
        .syntax = R"(
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number } [,...] | NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
)",
        .parent = "CREATE",
        .related = {"ALTER QUOTA", "CREATE SETTINGS PROFILE", "CREATE USER", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER QUOTA",
    {
        .description = R"DOCS_MD(
Changes quotas.

Syntax:

```sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time | queries_per_normalized_hash} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```
Keys `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address`, and `normalized_query_hash` correspond to the fields in the [system.quotas](/reference/system-tables/quotas) table.

`IPV4_PREFIX_BITS` and `IPV6_PREFIX_BITS` options can only be used when `KEYED BY` is `ip_address` or `forwarded_ip_address`. They correspond to the field in the [system.quotas](/reference/system-tables/quotas) table.

Parameters `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time`, `queries_per_normalized_hash` correspond to the fields in the [system.quotas_usage](/reference/system-tables/quotas_usage) table.

`ON CLUSTER` clause allows creating quotas on a cluster, see [Distributed DDL](/reference/statements/distributed-ddl).

**Examples**

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

For the default user limit the maximum execution time with half a second in 30 minutes, and limit the maximum number of queries with 321 and the maximum number of errors with 10 in 5 quarters:

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```
)DOCS_MD",
        .syntax = R"(
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number } [,...] | NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
)",
        .parent = "ALTER",
        .related = {"CREATE QUOTA", "ALTER", "SHOW"},
    });
}

}
