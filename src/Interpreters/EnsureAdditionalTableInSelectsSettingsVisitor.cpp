#include <Core/Settings.h>
#include <Interpreters/EnsureAdditionalTableInSelectsSettingsVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMap additional_table_filters;
}


EnsureAdditionalTableInSelectsSettingsVisitor::EnsureAdditionalTableInSelectsSettingsVisitor(const Map & additional_table_filters_)
    : additional_table_filters(additional_table_filters_)
{
}

void EnsureAdditionalTableInSelectsSettingsVisitor::visit(ASTPtr & ast)
{
    if (ast->as<ASTSelectQuery>())
        visitSelectQuery(ast);
    else
        visitChildren(ast);
}

void EnsureAdditionalTableInSelectsSettingsVisitor::visitSelectQuery(ASTPtr & ast)
{
    static constexpr std::string_view additional_table_filters_name = "additional_table_filters";

    auto & select_query_ast = ast->as<ASTSelectQuery &>();

    if (auto select_query_settings_ast = select_query_ast.settings())
    {
        auto & set_query = select_query_settings_ast->as<ASTSetQuery &>();
        if (!set_query.changes.tryGet(additional_table_filters_name))
            set_query.changes.setSetting(additional_table_filters_name, additional_table_filters);
    }
    else
    {
        auto set_query_ast = std::make_shared<ASTSetQuery>();
        set_query_ast->is_standalone = false;
        set_query_ast->changes.setSetting(additional_table_filters_name, additional_table_filters);
        select_query_ast.setExpression(ASTSelectQuery::Expression::SETTINGS, set_query_ast);
    }
}
void EnsureAdditionalTableInSelectsSettingsVisitor::visitChildren(ASTPtr & ast)
{
    for (auto & child : ast->children)
        visit(child);
}
}
