#include <Interpreters/ExpressionActionsSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
extern const SettingsBool compile_expressions;
extern const SettingsUInt64 max_temporary_columns;
extern const SettingsUInt64 max_temporary_non_const_columns;
extern const SettingsUInt64 min_count_to_compile_expression;
extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;

ExpressionActionsSettings ExpressionActionsSettings::fromSettings(const Settings & from, CompileExpressions compile_expressions_v)
{
    ExpressionActionsSettings settings;
    settings.can_compile_expressions = from[compile_expressions];
    settings.min_count_to_compile_expression_v = from[min_count_to_compile_expression];
    settings.max_temporary_columns_v = from[max_temporary_columns];
    settings.max_temporary_non_const_columns_v = from[max_temporary_non_const_columns];
    settings.compile_expressions_v = compile_expressions_v;
    settings.short_circuit_function_evaluation_v = from[short_circuit_function_evaluation];

    return settings;
}

ExpressionActionsSettings ExpressionActionsSettings::fromContext(ContextPtr from, CompileExpressions compile_expressions_v)
{
    return fromSettings(from->getSettingsRef(), compile_expressions_v);
}

}
