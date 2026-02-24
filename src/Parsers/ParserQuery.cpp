#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserCreateIndexQuery.h>
#include <Parsers/ParserDropFunctionQuery.h>
#include <Parsers/ParserDropWorkloadQuery.h>
#include <Parsers/ParserDropResourceQuery.h>
#include <Parsers/ParserDropIndexQuery.h>
#include <Parsers/ParserDropNamedCollectionQuery.h>
#include <Parsers/ParserAlterNamedCollectionQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserParallelWithQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserTransactionControl.h>
#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserCopyQuery.h>

#include <Parsers/Access/ParserCreateQuotaQuery.h>
#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ParserCreateRowPolicyQuery.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ParserDropAccessEntityQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/Access/ParserCheckGrantQuery.h>
#include <Parsers/Access/ParserMoveAccessEntityQuery.h>
#include <Parsers/Access/ParserSetRoleQuery.h>
#include <Parsers/Access/ParserExecuteAsQuery.h>


namespace DB
{

bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// QueryWithOutput includes SELECT, SELECT with UNION ALL, SHOW, and similar:
    ParserQueryWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);

    ParserInsertQuery insert_p(end, allow_settings_after_format_in_insert);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserSystemQuery system_p;
    ParserCreateUserQuery create_user_p;
    ParserCreateRoleQuery create_role_p;
    ParserCreateQuotaQuery create_quota_p;
    ParserCreateRowPolicyQuery create_row_policy_p;
    ParserCreateSettingsProfileQuery create_settings_profile_p;
    ParserCreateFunctionQuery create_function_p;
    ParserDropFunctionQuery drop_function_p;
    ParserCreateWorkloadQuery create_workload_p;
    ParserDropWorkloadQuery drop_workload_p;
    ParserCreateResourceQuery create_resource_p;
    ParserDropResourceQuery drop_resource_p;
    ParserCreateNamedCollectionQuery create_named_collection_p;
    ParserDropNamedCollectionQuery drop_named_collection_p;
    ParserAlterNamedCollectionQuery alter_named_collection_p;
    ParserCreateIndexQuery create_index_p;
    ParserDropIndexQuery drop_index_p;
    ParserDropAccessEntityQuery drop_access_entity_p;
    ParserMoveAccessEntityQuery move_access_entity_p;
    ParserGrantQuery grant_p;
    ParserCheckGrantQuery check_grant_p;
    ParserSetRoleQuery set_role_p;
    ParserTransactionControl transaction_control_p;
    ParserDeleteQuery delete_p;
    ParserUpdateQuery update_p;
    ParserCopyQuery copy_p;

    bool res = query_with_output_p.parse(pos, node, expected)
        || insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected)
        || set_role_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected)
        || system_p.parse(pos, node, expected)
        || create_user_p.parse(pos, node, expected)
        || create_role_p.parse(pos, node, expected)
        || create_quota_p.parse(pos, node, expected)
        || create_row_policy_p.parse(pos, node, expected)
        || create_settings_profile_p.parse(pos, node, expected)
        || create_function_p.parse(pos, node, expected)
        || drop_function_p.parse(pos, node, expected)
        || create_workload_p.parse(pos, node, expected)
        || drop_workload_p.parse(pos, node, expected)
        || create_resource_p.parse(pos, node, expected)
        || drop_resource_p.parse(pos, node, expected)
        || create_named_collection_p.parse(pos, node, expected)
        || drop_named_collection_p.parse(pos, node, expected)
        || alter_named_collection_p.parse(pos, node, expected)
        || create_index_p.parse(pos, node, expected)
        || drop_index_p.parse(pos, node, expected)
        || drop_access_entity_p.parse(pos, node, expected)
        || move_access_entity_p.parse(pos, node, expected)
        || grant_p.parse(pos, node, expected)
        || check_grant_p.parse(pos, node, expected)
        || transaction_control_p.parse(pos, node, expected)
        || delete_p.parse(pos, node, expected)
        || update_p.parse(pos, node, expected)
        || copy_p.parse(pos, node, expected);

    if (!res && allow_execute_as)
    {
        ParserQuery subquery_p{end, allow_settings_after_format_in_insert, implicit_select};
        subquery_p.allow_execute_as = false;
        ParserExecuteAsQuery execute_as_p{subquery_p};
        res = execute_as_p.parse(pos, node, expected);
    }

    if (res && allow_in_parallel_with)
    {
        ParserQuery subquery_p{end, allow_settings_after_format_in_insert, implicit_select};
        subquery_p.allow_in_parallel_with = false;
        ParserParallelWithQuery in_parallel_with_query_p(subquery_p, node);
        in_parallel_with_query_p.parse(pos, node, expected);
    }

    if (!res && implicit_select)
    {
        /// SELECT queries are already attempted to parse by ParserQueryWithOutput,
        /// but here we also try "implicit SELECT" after all other options.
        /// It allows to use ClickHouse as a calculator, to process queries like `1 + 2` without the SELECT keyword.
        ParserSelectQuery implicit_select_p(true);
        res = implicit_select_p.parse(pos, node, expected);
    }

    return res;
}

}
