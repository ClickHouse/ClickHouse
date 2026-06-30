#include <Common/SettingsChanges.h>
#include <Parsers/InsertQuerySettingsPushDownVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{

bool InsertQuerySettingsPushDownMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    if (node->as<ASTSelectWithUnionQuery>())
        return true;
    if (node->as<ASTSubquery>())
        return true;
    if (child->as<ASTSelectQuery>())
        return true;
    return false;
}

void InsertQuerySettingsPushDownMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * select_query = ast->as<ASTSelectQuery>())
        visit(*select_query, ast, data);
}

void InsertQuerySettingsPushDownMatcher::visit(ASTSelectQuery & select_query, ASTPtr &, Data & data)
{
    ASTPtr select_settings_ast = select_query.settings();
    if (!select_settings_ast)
        return;

    auto & insert_settings_ast = data.insert_settings_ast;

    if (!insert_settings_ast)
    {
        insert_settings_ast = select_settings_ast->clone();
        return;
    }

    SettingsChanges & select_settings = select_settings_ast->as<ASTSetQuery &>().changes;
    SettingsChanges & insert_settings = insert_settings_ast->as<ASTSetQuery &>().changes;
    auto & select_default_settings = select_settings_ast->as<ASTSetQuery &>().default_settings;
    auto & insert_default_settings = insert_settings_ast->as<ASTSetQuery &>().default_settings;

    std::unordered_set<String> insert_setting_names;
    insert_setting_names.reserve(insert_settings.size() + insert_default_settings.size());
    for (const auto & setting : insert_settings)
        insert_setting_names.insert(setting.name);
    for (const auto & setting_name : insert_default_settings)
        insert_setting_names.insert(setting_name);

    for (auto & setting : select_settings)
    {
        if (!insert_setting_names.contains(setting.name))
        {
            insert_settings.push_back(setting);
            insert_setting_names.insert(setting.name);
        }
        else
        {
            /// Do not overwrite setting that was passed for INSERT
            /// by settings that was passed for SELECT
        }
    }

    for (const auto & setting_name : select_default_settings)
    {
        if (!insert_setting_names.contains(setting_name))
        {
            insert_default_settings.push_back(setting_name);
            insert_setting_names.insert(setting_name);
        }
    }
}

}
