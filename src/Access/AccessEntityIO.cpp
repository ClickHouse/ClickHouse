#include <Access/AccessEntityIO.h>
#include <Access/IAccessEntity.h>
#include <Access/IAccessStorage.h>
#include <Access/Quota.h>
#include <Access/Role.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>
#include <Access/User.h>
#include <Core/Defines.h>
#include <Interpreters/InterpreterCreateQuotaQuery.h>
#include <Interpreters/InterpreterCreateRoleQuery.h>
#include <Interpreters/InterpreterCreateRowPolicyQuery.h>
#include <Interpreters/InterpreterCreateSettingsProfileQuery.h>
#include <Interpreters/InterpreterCreateUserQuery.h>
#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTCreateUserQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ParserCreateSettingsProfileQuery.h>
#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ParserGrantQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_ACCESS_ENTITY_DEFINITION;
}

using EntityType = IAccessStorage::EntityType;
using EntityTypeInfo = IAccessStorage::EntityTypeInfo;

namespace
{
    /// Special parser for the 'ATTACH access entity' queries.
    class ParserAttachAccessEntity : public IParserBase
    {
    protected:
        const char * getName() const override { return "ATTACH access entity query"; }

        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
        {
            ParserCreateUserQuery create_user_p;
            ParserCreateRoleQuery create_role_p;
            ParserCreateRowPolicyQuery create_policy_p;
            ParserCreateQuotaQuery create_quota_p;
            ParserCreateSettingsProfileQuery create_profile_p;
            ParserGrantQuery grant_p;

            create_user_p.useAttachMode();
            create_role_p.useAttachMode();
            create_policy_p.useAttachMode();
            create_quota_p.useAttachMode();
            create_profile_p.useAttachMode();
            grant_p.useAttachMode();

            return create_user_p.parse(pos, node, expected) || create_role_p.parse(pos, node, expected)
                || create_policy_p.parse(pos, node, expected) || create_quota_p.parse(pos, node, expected)
                || create_profile_p.parse(pos, node, expected) || grant_p.parse(pos, node, expected);
        }
    };

}


String serializeAccessEntity(const IAccessEntity & entity)
{
    /// Build list of ATTACH queries.
    ASTs queries;
    queries.push_back(InterpreterShowCreateAccessEntityQuery::getAttachQuery(entity));
    if ((entity.getType() == EntityType::USER) || (entity.getType() == EntityType::ROLE))
        boost::range::push_back(queries, InterpreterShowGrantsQuery::getAttachGrantQueries(entity));

    /// Serialize the list of ATTACH queries to a string.
    WriteBufferFromOwnString buf;
    for (const ASTPtr & query : queries)
    {
        formatAST(*query, buf, false, true);
        buf.write(";\n", 2);
    }
    return buf.str();
}

AccessEntityPtr deserializeAccessEntity(const String & definition, const String & path)
{
    ASTs queries;
    ParserAttachAccessEntity parser;
    const char * begin = definition.data(); /// begin of current query
    const char * pos = begin; /// parser moves pos from begin to the end of current query
    const char * end = begin + definition.size();
    while (pos < end)
    {
        queries.emplace_back(parseQueryAndMovePosition(parser, pos, end, "", true, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH));
        while (isWhitespaceASCII(*pos) || *pos == ';')
            ++pos;
    }

    /// Interpret the AST to build an access entity.
    std::shared_ptr<User> user;
    std::shared_ptr<Role> role;
    std::shared_ptr<RowPolicy> policy;
    std::shared_ptr<Quota> quota;
    std::shared_ptr<SettingsProfile> profile;
    AccessEntityPtr res;

    for (const auto & query : queries)
    {
        if (auto * create_user_query = query->as<ASTCreateUserQuery>())
        {
            if (res)
                throw Exception("Two access entities attached in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
            res = user = std::make_unique<User>();
            InterpreterCreateUserQuery::updateUserFromQuery(*user, *create_user_query);
        }
        else if (auto * create_role_query = query->as<ASTCreateRoleQuery>())
        {
            if (res)
                throw Exception("Two access entities attached in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
            res = role = std::make_unique<Role>();
            InterpreterCreateRoleQuery::updateRoleFromQuery(*role, *create_role_query);
        }
        else if (auto * create_policy_query = query->as<ASTCreateRowPolicyQuery>())
        {
            if (res)
                throw Exception("Two access entities attached in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
            res = policy = std::make_unique<RowPolicy>();
            InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(*policy, *create_policy_query);
        }
        else if (auto * create_quota_query = query->as<ASTCreateQuotaQuery>())
        {
            if (res)
                throw Exception("Two access entities attached in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
            res = quota = std::make_unique<Quota>();
            InterpreterCreateQuotaQuery::updateQuotaFromQuery(*quota, *create_quota_query);
        }
        else if (auto * create_profile_query = query->as<ASTCreateSettingsProfileQuery>())
        {
            if (res)
                throw Exception("Two access entities attached in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
            res = profile = std::make_unique<SettingsProfile>();
            InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(*profile, *create_profile_query);
        }
        else if (auto * grant_query = query->as<ASTGrantQuery>())
        {
            if (!user && !role)
                throw Exception(
                    "A user or role should be attached before grant in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
            if (user)
                InterpreterGrantQuery::updateUserFromQuery(*user, *grant_query);
            else
                InterpreterGrantQuery::updateRoleFromQuery(*role, *grant_query);
        }
        else
            throw Exception("No interpreter found for query " + query->getID(), ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);
    }

    if (!res)
        throw Exception("No access entities attached in " + path, ErrorCodes::INCORRECT_ACCESS_ENTITY_DEFINITION);

    return res;
}

}
