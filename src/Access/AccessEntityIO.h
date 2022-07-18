#pragma once

#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ParserCreateQuotaQuery.h>
#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ParserCreateRowPolicyQuery.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <base/types.h>
#include <memory>

namespace DB
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

struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

String serializeAccessEntity(const IAccessEntity & entity);

AccessEntityPtr deserializeAccessEntity(const String & definition, const String & file_path = "");

}
