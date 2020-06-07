#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSetRoleQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ParserCreateSettingsProfileQuery.h>
#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ParserGrantQuery.h>


namespace DB
{


bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserQueryWithOutput query_with_output_p(enable_explain);
    ParserInsertQuery insert_p(end);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserSystemQuery system_p;
    ParserCreateUserQuery create_user_p;
    ParserCreateRoleQuery create_role_p;
    ParserCreateQuotaQuery create_quota_p;
    ParserCreateRowPolicyQuery create_row_policy_p;
    ParserCreateSettingsProfileQuery create_settings_profile_p;
    ParserDropAccessEntityQuery drop_access_entity_p;
    ParserGrantQuery grant_p;
    ParserSetRoleQuery set_role_p;

    bool res = query_with_output_p.parse(pos, node, expected, ranges)
        || insert_p.parse(pos, node, expected, ranges)
        || use_p.parse(pos, node, expected, ranges)
        || set_role_p.parse(pos, node, expected, ranges)
        || set_p.parse(pos, node, expected, ranges)
        || system_p.parse(pos, node, expected, ranges)
        || create_user_p.parse(pos, node, expected, ranges)
        || create_role_p.parse(pos, node, expected, ranges)
        || create_quota_p.parse(pos, node, expected, ranges)
        || create_row_policy_p.parse(pos, node, expected, ranges)
        || create_settings_profile_p.parse(pos, node, expected, ranges)
        || drop_access_entity_p.parse(pos, node, expected, ranges)
        || grant_p.parse(pos, node, expected, ranges);

    return res;
}

}
