#include <Common/SettingsChanges.h>
#include <Parsers/InsertQuerySettingsPushDownVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>

#include <iterator>
#include <algorithm>

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

    for (auto & setting : select_settings)
    {
        auto it = std::find_if(insert_settings.begin(), insert_settings.end(), [&](auto & select_setting)
        {
            return select_setting.name == setting.name;
        });
        if (it == insert_settings.end())
            insert_settings.push_back(setting);
        else
        {
            /// Do not ovewrite setting that was passed for INSERT
            /// by settings that was passed for SELECT
        }
    }
}

}
