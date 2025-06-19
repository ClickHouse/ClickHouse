#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Storages/AlterCommands.h>
#include "SQLInterceptor.h"

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_sql_intercept_for_update_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_update_on_cluster;
    extern const SettingsBool enable_sql_intercept_for_delete_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_delete_on_cluster;
    extern const SettingsBool enable_sql_intercept_for_add_column_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_add_column_on_cluster;
    extern const SettingsBool enable_sql_intercept_for_drop_column_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_drop_column_on_cluster;
    extern const SettingsBool enable_sql_intercept_for_optimize_final_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_optimize_final_on_cluster;
    extern const SettingsBool enable_sql_intercept_for_drop_table_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_drop_table_on_cluster;
    extern const SettingsBool enable_sql_intercept_for_truncate_table_on_cluster;
    extern const SettingsSQLInterceptMode sql_intercept_mode_for_truncate_table_on_cluster;
}

namespace ErrorCodes
{
    extern const int QUERY_IS_PROHIBITED;
}

static LoggerPtr log = getLogger("SQLInterceptor");

void SQLInterceptor::intercept(const ASTPtr &  ast_ptr, ContextPtr context_ptr)
{
    if (auto * alter = ast_ptr->as<ASTAlterQuery>())
        alterIntercept(*alter, context_ptr);
    else if (auto * drop = ast_ptr->as<ASTDropQuery>())
        dropIntercept(*drop, context_ptr);
    else if (auto * optimize = ast_ptr->as<ASTOptimizeQuery>())
        optimizeIntercept(*optimize, context_ptr);
}

void SQLInterceptor::optimizeIntercept(const ASTOptimizeQuery & optimize, ContextPtr context_ptr)
{
    if (!optimize.cluster.empty() && optimize.final && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_optimize_final_on_cluster])
    {
        if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_optimize_final_on_cluster] == SQLInterceptMode::THROW)
            throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "OPTIMIZE FINAL mutations not allowed with ON CLUSTER");
        else
            LOG_WARNING(log, "OPTIMIZE FINAL mutations with ON CLUSTER is not desirable");
    }
}

void SQLInterceptor::dropIntercept(const ASTDropQuery & drop, ContextPtr context_ptr)
{
    switch (drop.kind)
    {
        case ASTDropQuery::Kind::Drop:
            if (!drop.cluster.empty() && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_drop_table_on_cluster])
            {
                if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_drop_table_on_cluster] == SQLInterceptMode::THROW)
                    throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "DROP TABLE mutations not allowed with ON CLUSTER");
                else
                    LOG_WARNING(log, "DROP TABLE mutations with ON CLUSTER is not desirable");
            }
            break;
        case ASTDropQuery::Kind::Truncate:
            if (!drop.cluster.empty() && !drop.is_view && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_truncate_table_on_cluster])
            {
                if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_truncate_table_on_cluster] == SQLInterceptMode::THROW)
                    throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "TRUNCATE TABLE mutations not allowed with ON CLUSTER");
                else
                    LOG_WARNING(log, "TRUNCATE TABLE mutations with ON CLUSTER is not desirable");
            }
            break;
        default:
            return;
    }
    
}

void SQLInterceptor::alterIntercept(const ASTAlterQuery & alter, ContextPtr context_ptr)
{
    for (auto & child : alter.command_list->children)
    {
        auto *  command_ast = child->as<ASTAlterCommand>();
        switch (command_ast->type)
        {
            case ASTAlterCommand::UPDATE:
                if (!alter.cluster.empty() && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_update_on_cluster])
                {
                    if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_update_on_cluster] == SQLInterceptMode::THROW)
                        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "UPDATE mutations not allowed with ON CLUSTER");
                    else
                        LOG_WARNING(log, "UPDATE mutations with ON CLUSTER is not desirable");
                }
                break;
            case ASTAlterCommand::DELETE:
                if (!alter.cluster.empty() && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_delete_on_cluster])
                {
                    if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_delete_on_cluster] == SQLInterceptMode::THROW)
                        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "DELETE mutations not allowed with ON CLUSTER");
                    else
                        LOG_WARNING(log, "DELETE mutations with ON CLUSTER is not desirable");
                }
                break;
            case ASTAlterCommand::ADD_COLUMN:
                if (!alter.cluster.empty() && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_add_column_on_cluster])
                {
                    if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_add_column_on_cluster] == SQLInterceptMode::THROW)
                        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "ADD COLUMN mutations not allowed with ON CLUSTER");
                    else
                        LOG_WARNING(log, "ADD COLUMN mutations with ON CLUSTER is not desirable");
                }
                break;
            case ASTAlterCommand::DROP_COLUMN:
                if (!alter.cluster.empty() && context_ptr->getSettingsRef()[Setting::enable_sql_intercept_for_drop_column_on_cluster])
                {
                    if (context_ptr->getSettingsRef()[Setting::sql_intercept_mode_for_drop_column_on_cluster] == SQLInterceptMode::THROW)
                        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "DROP COLUMN mutations not allowed with ON CLUSTER");
                    else
                        LOG_WARNING(log, "DROP COLUMN mutations with ON CLUSTER is not desirable");
                }
                break;
            default:
                break;
        }
    }

}

}
