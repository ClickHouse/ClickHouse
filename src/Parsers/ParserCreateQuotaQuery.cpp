#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIntervalKind.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <ext/range.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


namespace
{
    using KeyType = Quota::KeyType;
    using KeyTypeInfo = Quota::KeyTypeInfo;
    using ResourceType = Quota::ResourceType;
    using ResourceTypeInfo = Quota::ResourceTypeInfo;
    using ResourceAmount = Quota::ResourceAmount;


    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseKeyType(IParserBase::Pos & pos, Expected & expected, std::optional<Quota::KeyType> & key_type)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"KEYED BY"}.ignore(pos, expected))
                return false;

            ASTPtr key_type_ast;
            if (!ParserStringLiteral().parse(pos, key_type_ast, expected))
                return false;

            const String & key_type_str = key_type_ast->as<ASTLiteral &>().value.safeGet<const String &>();
            for (auto kt : ext::range(Quota::KeyType::MAX))
                if (boost::iequals(KeyTypeInfo::get(kt).name, key_type_str))
                {
                    key_type = kt;
                    return true;
                }

            String all_key_types_str;
            for (auto kt : ext::range(Quota::KeyType::MAX))
                all_key_types_str += String(all_key_types_str.empty() ? "" : ", ") + "'" + KeyTypeInfo::get(kt).name + "'";
            String msg = "Quota cannot be keyed by '" + key_type_str + "'. Expected one of these literals: " + all_key_types_str;
            throw Exception(msg, ErrorCodes::SYNTAX_ERROR);
        });
    }

    bool parseLimit(IParserBase::Pos & pos, Expected & expected, bool first, ResourceType & resource_type, ResourceAmount & max)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (first)
            {
                if (!ParserKeyword{"MAX"}.ignore(pos, expected))
                    return false;
            }
            else
            {
                if (!ParserToken{TokenType::Comma}.ignore(pos, expected))
                    return false;

                ParserKeyword{"MAX"}.ignore(pos, expected);
            }

            std::optional<ResourceType> res_resource_type;
            for (auto rt : ext::range(Quota::MAX_RESOURCE_TYPE))
            {
                if (ParserKeyword{ResourceTypeInfo::get(rt).keyword.c_str()}.ignore(pos, expected))
                {
                    res_resource_type = rt;
                    break;
                }
            }
            if (!res_resource_type)
                return false;

            ResourceAmount res_max;
            ASTPtr max_ast;
            if (ParserNumber{}.parse(pos, max_ast, expected))
            {
                const Field & max_field = max_ast->as<ASTLiteral &>().value;
                const auto & type_info = ResourceTypeInfo::get(*res_resource_type);
                if (type_info.output_denominator == 1)
                    res_max = applyVisitor(FieldVisitorConvertToNumber<ResourceAmount>(), max_field);
                else
                    res_max = static_cast<ResourceAmount>(
                        applyVisitor(FieldVisitorConvertToNumber<double>(), max_field) * type_info.output_denominator);
            }
            else
                return false;

            resource_type = *res_resource_type;
            max = res_max;
            return true;
        });
    }

    bool parseLimits(IParserBase::Pos & pos, Expected & expected, ASTCreateQuotaQuery::Limits & limits)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTCreateQuotaQuery::Limits new_limits;
            if (!ParserKeyword{"FOR"}.ignore(pos, expected))
                return false;

            new_limits.randomize_interval = ParserKeyword{"RANDOMIZED"}.ignore(pos, expected);

            if (!ParserKeyword{"INTERVAL"}.ignore(pos, expected))
                return false;

            ASTPtr num_intervals_ast;
            if (!ParserNumber{}.parse(pos, num_intervals_ast, expected))
                return false;

            double num_intervals = applyVisitor(FieldVisitorConvertToNumber<double>(), num_intervals_ast->as<ASTLiteral &>().value);

            IntervalKind interval_kind;
            if (!parseIntervalKind(pos, expected, interval_kind))
                return false;

            new_limits.duration = std::chrono::seconds(static_cast<UInt64>(num_intervals * interval_kind.toAvgSeconds()));

            if (ParserKeyword{"NO LIMITS"}.ignore(pos, expected))
            {
                new_limits.drop = true;
            }
            else if (ParserKeyword{"TRACKING ONLY"}.ignore(pos, expected))
            {
            }
            else
            {
                ResourceType resource_type;
                ResourceAmount max;
                if (!parseLimit(pos, expected, true, resource_type, max))
                    return false;

                new_limits.max[resource_type] = max;
                while (parseLimit(pos, expected, false, resource_type, max))
                    new_limits.max[resource_type] = max;
            }

            limits = new_limits;
            return true;
        });
    }

    bool parseAllLimits(IParserBase::Pos & pos, Expected & expected, std::vector<ASTCreateQuotaQuery::Limits> & all_limits)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            size_t old_size = all_limits.size();
            do
            {
                ASTCreateQuotaQuery::Limits limits;
                if (!parseLimits(pos, expected, limits))
                {
                    all_limits.resize(old_size);
                    return false;
                }
                all_limits.push_back(limits);
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            return true;
        });
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr node;
            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoleNames().allowUserNames().allowCurrentUser().useIDMode(id_mode);
            if (roles || !ParserKeyword{"TO"}.ignore(pos, expected) || !roles_p.parse(pos, node, expected))
                return false;

            roles = std::static_pointer_cast<ASTRolesOrUsersSet>(node);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateQuotaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{"ATTACH QUOTA"}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{"ALTER QUOTA"}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{"CREATE QUOTA"}.ignore(pos, expected))
            return false;
    }

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected))
            or_replace = true;
    }

    Strings names;
    if (!parseIdentifiersOrStringLiterals(pos, expected, names))
        return false;

    String new_name;
    std::optional<KeyType> key_type;
    std::vector<ASTCreateQuotaQuery::Limits> all_limits;
    String cluster;

    while (true)
    {
        if (alter && new_name.empty() && (names.size() == 1) && parseRenameTo(pos, expected, new_name))
            continue;

        if (!key_type && parseKeyType(pos, expected, key_type))
            continue;

        if (parseAllLimits(pos, expected, all_limits))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        break;
    }

    std::shared_ptr<ASTRolesOrUsersSet> roles;
    parseToRoles(pos, expected, attach_mode, roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = std::make_shared<ASTCreateQuotaQuery>();
    node = query;

    query->alter = alter;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_name = std::move(new_name);
    query->key_type = key_type;
    query->all_limits = std::move(all_limits);
    query->roles = std::move(roles);

    return true;
}
}
