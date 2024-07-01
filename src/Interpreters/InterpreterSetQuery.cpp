#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes, SettingSource::QUERY);
    auto session_context = getContext()->getSessionContext();
    session_context->applySettingsChanges(ast.changes);
    session_context->addQueryParameters(ast.query_parameters);
    session_context->resetSettingsToDefaultValue(ast.default_settings);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext(bool ignore_setting_constraints)
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    if (!ignore_setting_constraints)
        getContext()->checkSettingsConstraints(ast.changes, SettingSource::QUERY);
    getContext()->applySettingsChanges(ast.changes);
    getContext()->resetSettingsToDefaultValue(ast.default_settings);
}

static void applySettingsFromSelectWithUnion(const ASTSelectWithUnionQuery & select_with_union, ContextMutablePtr context)
{
    const ASTs & children = select_with_union.list_of_selects->children;
    if (children.empty())
        return;

    // We might have an arbitrarily complex UNION tree, so just give
    // up if the last first-order child is not a plain SELECT.
    // It is flattened later, when we process UNION ALL/DISTINCT.
    const auto * last_select = children.back()->as<ASTSelectQuery>();
    if (last_select && last_select->settings())
    {
        InterpreterSetQuery(last_select->settings(), context).executeForCurrentContext();
    }
}

void InterpreterSetQuery::applySettingsFromQuery(const ASTPtr & ast, ContextMutablePtr context_)
{
    if (!ast)
        return;

    if (const auto * select_query = ast->as<ASTSelectQuery>())
    {
        if (auto new_settings = select_query->settings())
            InterpreterSetQuery(new_settings, context_).executeForCurrentContext();
    }
    else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        applySettingsFromSelectWithUnion(*select_with_union_query, context_);
    }
    else if (const auto * explain_query = ast->as<ASTExplainQuery>())
    {
        if (explain_query->settings_ast)
            InterpreterSetQuery(explain_query->settings_ast, context_).executeForCurrentContext();

        applySettingsFromQuery(explain_query->getExplainedQuery(), context_);
    }
    else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
    {
        if (query_with_output->settings_ast)
            InterpreterSetQuery(query_with_output->settings_ast, context_).executeForCurrentContext();

        if (const auto * create_query = ast->as<ASTCreateQuery>())
        {
            if (create_query->select)
            {
                applySettingsFromSelectWithUnion(create_query->select->as<ASTSelectWithUnionQuery &>(), context_);
            }
        }
    }
    else if (auto * insert_query = ast->as<ASTInsertQuery>())
    {
        context_->setInsertFormat(insert_query->format);
        if (insert_query->settings_ast)
            InterpreterSetQuery(insert_query->settings_ast, context_).executeForCurrentContext();
    }
}

}
