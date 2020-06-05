#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIntervalKind.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
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

    bool parseKeyType(IParserBase::Pos & pos, Expected & expected, KeyType & key_type)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword{"NOT KEYED"}.ignore(pos, expected))
            {
                key_type = KeyType::NONE;
                return true;
            }

            if (!ParserKeyword{"KEY BY"}.ignore(pos, expected) && !ParserKeyword{"KEYED BY"}.ignore(pos, expected))
                return false;

            Strings names;
            if (!parseIdentifiersOrStringLiterals(pos, expected, names))
                return false;

            String name = boost::algorithm::join(names, "_or_");
            boost::to_lower(name);
            boost::replace_all(name, " ", "_");

            for (auto kt : ext::range(Quota::KeyType::MAX))
                if (KeyTypeInfo::get(kt).name == name)
                {
                    key_type = kt;
                    return true;
                }

            String all_types_str;
            for (auto kt : ext::range(Quota::KeyType::MAX))
                all_types_str += String(all_types_str.empty() ? "" : ", ") + "'" + KeyTypeInfo::get(kt).name + "'";
            String msg = "Quota cannot be keyed by '" + name + "'. Expected one of the following identifiers: " + all_types_str;
            throw Exception(msg, ErrorCodes::SYNTAX_ERROR);
        });
    }


    bool parseResourceType(IParserBase::Pos & pos, Expected & expected, ResourceType & resource_type)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            for (auto rt : ext::range(Quota::MAX_RESOURCE_TYPE))
            {
                if (ParserKeyword{ResourceTypeInfo::get(rt).keyword.c_str()}.ignore(pos, expected))
                {
                    resource_type = rt;
                    return true;
                }
            }

            ASTPtr ast;
            if (!ParserIdentifier{}.parse(pos, ast, expected))
                return false;

            String name = getIdentifierName(ast);
            for (auto rt : ext::range(Quota::MAX_RESOURCE_TYPE))
            {
                if (ResourceTypeInfo::get(rt).name == name)
                {
                    resource_type = rt;
                    return true;
                }
            }

            return false;
        });
    }


    bool parseMaxAmount(IParserBase::Pos & pos, Expected & expected, ResourceType resource_type, ResourceAmount & max)
    {
        ASTPtr ast;
        if (!ParserNumber{}.parse(pos, ast, expected))
            return false;

        const Field & max_field = ast->as<ASTLiteral &>().value;
        const auto & type_info = ResourceTypeInfo::get(resource_type);
        if (type_info.output_denominator == 1)
            max = applyVisitor(FieldVisitorConvertToNumber<ResourceAmount>(), max_field);
        else
            max = static_cast<ResourceAmount>(
                applyVisitor(FieldVisitorConvertToNumber<double>(), max_field) * type_info.output_denominator);
        return true;
    }


    bool parseLimit(IParserBase::Pos & pos, Expected & expected, bool first, bool & max_prefix_encountered, ResourceType & resource_type, ResourceAmount & max)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!first && !ParserToken{TokenType::Comma}.ignore(pos, expected))
                return false;

            max_prefix_encountered |= ParserKeyword{"MAX"}.ignore(pos, expected);

            ResourceType res_resource_type;
            if (!parseResourceType(pos, expected, res_resource_type))
                return false;

            if (max_prefix_encountered)
            {
                ParserToken{TokenType::Equals}.ignore(pos, expected);
            }
            else
            {
                if (!ParserKeyword{"MAX"}.ignore(pos, expected))
                    return false;
            }

            ResourceAmount res_max;
            if (!parseMaxAmount(pos, expected, res_resource_type, res_max))
                return false;

            resource_type = res_resource_type;
            max = res_max;
            return true;
        });
    }

    bool parseIntervalWithLimits(IParserBase::Pos & pos, Expected & expected, ASTCreateQuotaQuery::Limits & limits)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTCreateQuotaQuery::Limits new_limits;
            if (!ParserKeyword{"FOR"}.ignore(pos, expected))
                return false;

            new_limits.randomize_interval = ParserKeyword{"RANDOMIZED"}.ignore(pos, expected);

            ParserKeyword{"INTERVAL"}.ignore(pos, expected);

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
                bool max_prefix_encountered = false;
                if (!parseLimit(pos, expected, true, max_prefix_encountered, resource_type, max))
                    return false;

                new_limits.max[resource_type] = max;
                while (parseLimit(pos, expected, false, max_prefix_encountered, resource_type, max))
                    new_limits.max[resource_type] = max;
            }

            limits = new_limits;
            return true;
        });
    }

    bool parseIntervalsWithLimits(IParserBase::Pos & pos, Expected & expected, std::vector<ASTCreateQuotaQuery::Limits> & all_limits)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            size_t old_size = all_limits.size();
            do
            {
                ASTCreateQuotaQuery::Limits limits;
                if (!parseIntervalWithLimits(pos, expected, limits))
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
            if (!ParserKeyword{"TO"}.ignore(pos, expected) || !roles_p.parse(pos, node, expected))
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

        if (!key_type)
        {
            KeyType new_key_type;
            if (parseKeyType(pos, expected, new_key_type))
            {
                key_type = new_key_type;
                continue;
            }
        }

        if (parseIntervalsWithLimits(pos, expected, all_limits))
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
