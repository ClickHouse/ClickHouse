#include <Common/SettingsChanges.h>
#include <Parsers/QueryWithOutputSettingsPushDownVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>

#include <iterator>
#include <algorithm>

namespace DB
{

bool QueryWithOutputSettingsPushDownMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    if (node->as<ASTSelectWithUnionQuery>())
        return true;
    if (node->as<ASTSubquery>())
        return true;
    if (child->as<ASTSelectQuery>())
        return true;
    return false;
}

void QueryWithOutputSettingsPushDownMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * select_query = ast->as<ASTSelectQuery>())
        visit(*select_query, ast, data);
}

void QueryWithOutputSettingsPushDownMatcher::visit(ASTSelectQuery & select_query, ASTPtr &, Data & data)
{
    ASTPtr select_settings_ast = select_query.settings();
    if (!select_settings_ast)
    {
        select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, data.settings_ast->clone());
        return;
    }

    SettingsChanges & select_settings = select_settings_ast->as<ASTSetQuery &>().changes;
    SettingsChanges & settings = data.settings_ast->as<ASTSetQuery &>().changes;

    for (auto & setting : settings)
    {
        auto it = std::find_if(select_settings.begin(), select_settings.end(), [&](auto & select_setting)
        {
            return select_setting.name == setting.name;
        });
        if (it == select_settings.end())
            select_settings.push_back(setting);
        else
            it->value = setting.value;
    }
}

}
