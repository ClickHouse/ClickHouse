#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIntervalKind.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserRoleList.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTRoleList.h>
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
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;

    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name, bool alter)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!new_name.empty() || !alter)
                return false;

            if (!ParserKeyword{"RENAME TO"}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseKeyType(IParserBase::Pos & pos, Expected & expected, std::optional<Quota::KeyType> & key_type)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (key_type)
                return false;

            if (!ParserKeyword{"KEYED BY"}.ignore(pos, expected))
                return false;

            ASTPtr key_type_ast;
            if (!ParserStringLiteral().parse(pos, key_type_ast, expected))
                return false;

            const String & key_type_str = key_type_ast->as<ASTLiteral &>().value.safeGet<const String &>();
            for (auto kt : ext::range_with_static_cast<Quota::KeyType>(Quota::MAX_KEY_TYPE))
                if (boost::iequals(Quota::getNameOfKeyType(kt), key_type_str))
                {
                    key_type = kt;
                    return true;
                }

            String all_key_types_str;
            for (auto kt : ext::range_with_static_cast<Quota::KeyType>(Quota::MAX_KEY_TYPE))
                all_key_types_str += String(all_key_types_str.empty() ? "" : ", ") + "'" + Quota::getNameOfKeyType(kt) + "'";
            String msg = "Quota cannot be keyed by '" + key_type_str + "'. Expected one of these literals: " + all_key_types_str;
            throw Exception(msg, ErrorCodes::SYNTAX_ERROR);
        });
    }

    bool parseLimit(IParserBase::Pos & pos, Expected & expected, ResourceType & resource_type, ResourceAmount & max)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"MAX"}.ignore(pos, expected))
                return false;

            bool resource_type_set = false;
            for (auto rt : ext::range_with_static_cast<Quota::ResourceType>(Quota::MAX_RESOURCE_TYPE))
            {
                if (ParserKeyword{Quota::resourceTypeToKeyword(rt)}.ignore(pos, expected))
                {
                    resource_type = rt;
                    resource_type_set = true;
                    break;
                }
            }
            if (!resource_type_set)
                return false;

            if (!ParserToken{TokenType::Equals}.ignore(pos, expected))
                return false;

            ASTPtr max_ast;
            if (ParserNumber{}.parse(pos, max_ast, expected))
            {
                const Field & max_field = max_ast->as<ASTLiteral &>().value;
                if (resource_type == Quota::EXECUTION_TIME)
                    max = Quota::secondsToExecutionTime(applyVisitor(FieldVisitorConvertToNumber<double>(), max_field));
                else
                    max = applyVisitor(FieldVisitorConvertToNumber<ResourceAmount>(), max_field);
            }
            else if (ParserKeyword{"ANY"}.ignore(pos, expected))
            {
                max = Quota::UNLIMITED;
            }
            else
                return false;

            return true;
        });
    }

    bool parseCommaAndLimit(IParserBase::Pos & pos, Expected & expected, ResourceType & resource_type, ResourceAmount & max)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserToken{TokenType::Comma}.ignore(pos, expected))
                return false;

            return parseLimit(pos, expected, resource_type, max);
        });
    }

    bool parseLimits(IParserBase::Pos & pos, Expected & expected, ASTCreateQuotaQuery::Limits & limits, bool alter)
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

            if (alter && ParserKeyword{"UNSET TRACKING"}.ignore(pos, expected))
            {
                new_limits.unset_tracking = true;
            }
            else if (ParserKeyword{"SET TRACKING"}.ignore(pos, expected) || ParserKeyword{"TRACKING"}.ignore(pos, expected))
            {
            }
            else
            {
                ParserKeyword{"SET"}.ignore(pos, expected);
                ResourceType resource_type;
                ResourceAmount max;
                if (!parseLimit(pos, expected, resource_type, max))
                    return false;

                new_limits.max[resource_type] = max;
                while (parseCommaAndLimit(pos, expected, resource_type, max))
                    new_limits.max[resource_type] = max;
            }

            limits = new_limits;
            return true;
        });
    }

    bool parseAllLimits(IParserBase::Pos & pos, Expected & expected, std::vector<ASTCreateQuotaQuery::Limits> & all_limits, bool alter)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            do
            {
                ASTCreateQuotaQuery::Limits limits;
                if (!parseLimits(pos, expected, limits, alter))
                    return false;
                all_limits.push_back(limits);
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            return true;
        });
    }

    bool parseRoles(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTRoleList> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr node;
            if (roles || !ParserKeyword{"TO"}.ignore(pos, expected) || !ParserRoleList{}.parse(pos, node, expected))
                return false;

            roles = std::static_pointer_cast<ASTRoleList>(node);
            return true;
        });
    }
}


bool ParserCreateQuotaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter;
    if (ParserKeyword{"CREATE QUOTA"}.ignore(pos, expected))
        alter = false;
    else if (ParserKeyword{"ALTER QUOTA"}.ignore(pos, expected))
        alter = true;
    else
        return false;

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

    String name;
    if (!parseIdentifierOrStringLiteral(pos, expected, name))
        return false;

    String new_name;
    std::optional<KeyType> key_type;
    std::vector<ASTCreateQuotaQuery::Limits> all_limits;
    std::shared_ptr<ASTRoleList> roles;

    while (parseRenameTo(pos, expected, new_name, alter) || parseKeyType(pos, expected, key_type)
           || parseAllLimits(pos, expected, all_limits, alter) || parseRoles(pos, expected, roles))
        ;

    auto query = std::make_shared<ASTCreateQuotaQuery>();
    node = query;

    query->alter = alter;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->name = std::move(name);
    query->new_name = std::move(new_name);
    query->key_type = key_type;
    query->all_limits = std::move(all_limits);
    query->roles = std::move(roles);

    return true;
}
}
