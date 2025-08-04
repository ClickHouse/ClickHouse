#include <Interpreters/ExpressionActionsSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool compile_expressions;
    extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
    extern const SettingsUInt64 max_temporary_columns;
    extern const SettingsUInt64 max_temporary_non_const_columns;
    extern const SettingsUInt64 min_count_to_compile_expression;
}

ExpressionActionsSettings::ExpressionActionsSettings()
{
    can_compile_expressions = false;
    min_count_to_compile_expression = 0;

    max_temporary_columns = 0;
    max_temporary_non_const_columns = 0;

    compile_expressions = CompileExpressions::no;

    short_circuit_function_evaluation = ShortCircuitFunctionEvaluation::DISABLE;
}

ExpressionActionsSettings::ExpressionActionsSettings(const Settings & from, CompileExpressions compile_expressions_)
{
    can_compile_expressions = from[Setting::compile_expressions];
    min_count_to_compile_expression = from[Setting::min_count_to_compile_expression];
    max_temporary_columns = from[Setting::max_temporary_columns];
    max_temporary_non_const_columns = from[Setting::max_temporary_non_const_columns];
    compile_expressions = compile_expressions_;
    short_circuit_function_evaluation = from[Setting::short_circuit_function_evaluation];
}

ExpressionActionsSettings::ExpressionActionsSettings(ContextPtr from, CompileExpressions compile_expressions_)
    : ExpressionActionsSettings(from->getSettingsRef(), compile_expressions_)
{
}

}
