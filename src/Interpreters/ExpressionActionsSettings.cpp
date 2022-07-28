#include <Interpreters/ExpressionActionsSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

ExpressionActionsSettings ExpressionActionsSettings::fromSettings(const Settings & from, CompileExpressions compile_expressions)
{
    ExpressionActionsSettings settings;
    settings.can_compile_expressions = from.compile_expressions;
    settings.min_count_to_compile_expression = from.min_count_to_compile_expression;
    settings.max_temporary_columns = from.max_temporary_columns;
    settings.max_temporary_non_const_columns = from.max_temporary_non_const_columns;
    settings.compile_expressions = compile_expressions;

    return settings;
}

ExpressionActionsSettings ExpressionActionsSettings::fromContext(ContextPtr from, CompileExpressions compile_expressions)
{
    return fromSettings(from->getSettingsRef(), compile_expressions);
}

}
