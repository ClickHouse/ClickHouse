#include <Core/Settings.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Storages/AlterCommands.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/SQLInterceptor.h>
#include <gtest/gtest.h>

using namespace DB;

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
}

TEST(SQLInterceptor, testUpdateOnCluster)
{
    const std::string query = "alter table cktest.test2 on cluster default_cluster update CounterID = toUInt32(222) where EventDate='2023-08-01';";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_update_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_update_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_update_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_update_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_update_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_update_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}

TEST(SQLInterceptor, testDeleteOnCluster)
{
    const std::string query = "alter table cktest.test2 on cluster default_cluster delete where EventDate='2023-08-01';";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_delete_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_delete_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_delete_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_delete_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_delete_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_delete_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}

TEST(SQLInterceptor, testAddColumnOnCluster)
{
    const std::string query = "alter table cktest.test2 on cluster default_cluster add column EventDate Date;";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_add_column_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_add_column_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_add_column_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_add_column_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_add_column_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_add_column_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}

TEST(SQLInterceptor, testDropColumnOnCluster)
{
    const std::string query = "alter table cktest.test2 on cluster default_cluster drop column EventDate;";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_drop_column_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_drop_column_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_drop_column_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_drop_column_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_drop_column_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_drop_column_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}

TEST(SQLInterceptor, testOptimizeFinalOnCluster)
{
    const std::string query = "optimize table cktest.test2 on cluster default_cluster final DEDUPLICATE;";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_optimize_final_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_optimize_final_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_optimize_final_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_optimize_final_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_optimize_final_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_optimize_final_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}

TEST(SQLInterceptor, testDropTableOnCluster)
{
    const std::string query = "drop table cktest.test2 on cluster default_cluster;";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_drop_table_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_drop_table_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_drop_table_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_drop_table_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_drop_table_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_drop_table_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}

TEST(SQLInterceptor, TruncateTableOnCluster)
{
    const std::string query = "truncate table cktest.test2 on cluster default_cluster;";
    auto context = Context::createCopy(getContext().context);
    Settings settings = context->getSettingsCopy();
    settings[Setting::sql_intercept_mode_for_truncate_table_on_cluster] = DB::SQLInterceptMode::THROW;
    // Testing enable_sql_intercept_for_truncate_table_on_cluster default value is false
    context->setSettings(settings);
    const char * end = query.c_str() + query.size();
    ParserQuery parser(end);

    auto ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));

    // Testing enable_sql_intercept_for_truncate_table_on_cluster been set to true
    settings[Setting::enable_sql_intercept_for_truncate_table_on_cluster] = true;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_THROW(SQLInterceptor::intercept(ast, context), DB::Exception);

    // Testing sql_intercept_mode_for_truncate_table_on_cluster been set to LOG
    settings[Setting::sql_intercept_mode_for_truncate_table_on_cluster] = DB::SQLInterceptMode::LOG;
    context->setSettings(settings);
    ast = parseQuery(parser, query.c_str(), end, "", 1024, 1024, 1024);
    ASSERT_NO_THROW(SQLInterceptor::intercept(ast, context));
}
