#include <Parsers/ParserAttachAccessEntity.h>
#include <Parsers/Access/ParserCreateQuotaQuery.h>
#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ParserCreateRowPolicyQuery.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>

namespace DB
{

bool ParserAttachAccessEntity::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
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

}
