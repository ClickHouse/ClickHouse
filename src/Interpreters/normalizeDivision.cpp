#include <Interpreters/normalizeDivision.h>

#include <Core/Settings.h>
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

bool normalize(IAST & ast, bool exact)
{
    /// An explicit `ATTACH` definition may contain expressions from an older server.
    if (const auto * create = ast.as<ASTCreateQuery>(); create && create->attach)
        return false;

    if (const auto * select = ast.as<ASTSelectQuery>(); select && select->settings())
    {
        const auto & query_settings = select->settings()->as<ASTSetQuery &>();
        for (const auto & change : query_settings.changes)
        {
            if (change.name == "exact_division_for_integers")
            {
                Settings settings;
                settings.set(change.name, change.value);
                exact = settings[Setting::exact_division_for_integers];
            }
        }

        /// `InterpreterSetQuery` applies resets after explicit changes.
        for (const auto & name : query_settings.default_settings)
            if (name == "exact_division_for_integers")
                exact = Settings{}[Setting::exact_division_for_integers];
    }

    /// Type names and storage engine names are not scalar function calls.
    if (auto * column = ast.as<ASTColumnDeclaration>())
    {
        bool changed = false;
        for (const auto & expression : {column->getDefaultExpression(), column->getTTL()})
            if (expression)
                changed |= normalize(*expression, exact);
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
                changed |= normalize(*argument, exact);
        }
        else if (child.get() == select_list)
        {
            for (const auto & expression : child->children)
            {
                const auto * with_alias = dynamic_cast<const ASTWithAlias *>(expression.get());
                const String original_name = with_alias && with_alias->alias.empty() ? expression->getColumnName() : String{};
                if (normalize(*expression, exact))
                {
                    changed = true;
                    /// Views and outer queries refer to the original automatic column names.
                    if (!original_name.empty())
                        expression->setAlias(original_name);
                }
            }
        }
        else
            changed |= normalize(*child, exact);
    }

    /// These expressions are stored outside `children`.
    if (auto * ttl = ast.as<ASTTTLElement>())
    {
        for (const auto & key : ttl->group_by_key)
            changed |= normalize(*key, exact);
        for (const auto & assignment : ttl->group_by_assignments)
            changed |= normalize(*assignment, exact);
    }
    return changed;
}

}

void normalizeDivision(IAST & ast, bool exact_division_for_integers)
{
    normalize(ast, exact_division_for_integers);
}

}
