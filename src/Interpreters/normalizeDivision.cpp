#include <Interpreters/normalizeDivision.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool exact_division_for_integers;
}

namespace
{

bool normalize(IAST & ast, bool exact, const ContextPtr & context)
{
    /// An explicit `ATTACH` definition may contain expressions from an older server.
    if (const auto * create = ast.as<ASTCreateQuery>(); create && create->attach)
        return false;

    ContextPtr scope_context;
    if (const auto * select = ast.as<ASTSelectQuery>(); select && select->settings())
    {
        const auto & query_settings = select->settings()->as<ASTSetQuery &>();
        auto updated_context = Context::createCopy(context);
        auto changes = query_settings.changes;
        /// Match the per-query context in `QueryTreeBuilder`, including settings profiles.
        updated_context->clampToSettingsConstraints(changes, SettingSource::QUERY);
        updated_context->applySettingsChanges(changes);
        /// `InterpreterSetQuery` applies resets after explicit changes.
        updated_context->resetSettingsToDefaultValue(query_settings.default_settings);
        exact = updated_context->getSettingsRef()[Setting::exact_division_for_integers];
        scope_context = std::move(updated_context);
    }
    const ContextPtr & current_context = scope_context ? scope_context : context;

    /// Type names and storage engine names are not scalar function calls.
    if (auto * column = ast.as<ASTColumnDeclaration>())
    {
        bool changed = false;
        for (const auto & expression : {column->getDefaultExpression(), column->getTTL()})
            if (expression)
                changed |= normalize(*expression, exact, current_context);
        return changed;
    }

    bool changed = false;
    if (auto * function = ast.as<ASTFunction>(); exact && function && function->name == "divide")
    {
        function->name = "divideExact";
        function->setIsOperator(false);
        changed = true;
    }

    const auto * storage = ast.as<ASTStorage>();
    const auto * select = ast.as<ASTSelectQuery>();
    const IAST * select_list = select ? select->select().get() : nullptr;
    for (const auto & child : ast.children)
    {
        if (storage && child.get() == storage->engine)
        {
            for (const auto & argument : child->children)
                changed |= normalize(*argument, exact, current_context);
        }
        else if (child.get() == select_list)
        {
            for (const auto & expression : child->children)
            {
                const auto * with_alias = dynamic_cast<const ASTWithAlias *>(expression.get());
                const String original_name = with_alias && with_alias->alias.empty() ? expression->getColumnName() : String{};
                if (normalize(*expression, exact, current_context))
                {
                    changed = true;
                    /// Views and outer queries refer to the original automatic column names.
                    if (!original_name.empty())
                        expression->setAlias(original_name);
                }
            }
        }
        else
            changed |= normalize(*child, exact, current_context);
    }

    /// These expressions are stored outside `children`.
    if (auto * ttl = ast.as<ASTTTLElement>())
    {
        for (const auto & key : ttl->group_by_key)
            changed |= normalize(*key, exact, current_context);
        for (const auto & assignment : ttl->group_by_assignments)
            changed |= normalize(*assignment, exact, current_context);
    }
    return changed;
}

}

void normalizeDivision(IAST & ast, const ContextPtr & context)
{
    normalize(ast, context->getSettingsRef()[Setting::exact_division_for_integers], context);
}

}
