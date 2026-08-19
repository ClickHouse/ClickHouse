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
    extern const SettingsBool enable_lazy_columns_replication;
    extern const SettingsBool serialize_query_plan;
}

ExpressionActionsSettings::ExpressionActionsSettings(const Settings & from, CompileExpressions compile_expressions_)
{
    /// A JIT-fused function's getName() returns a dump of its sub-expression, not a real
    /// function name (see LLVMFunction in ExpressionJIT.cpp) -- serialize() would ship that
    /// dump as if it were resolvable, so don't let fusion happen on plans that might be sent
    /// remotely (issue #115310).
    can_compile_expressions = from[Setting::compile_expressions] && !from[Setting::serialize_query_plan];
    min_count_to_compile_expression = from[Setting::min_count_to_compile_expression];
    max_temporary_columns = from[Setting::max_temporary_columns];
    max_temporary_non_const_columns = from[Setting::max_temporary_non_const_columns];
    compile_expressions = compile_expressions_;
    short_circuit_function_evaluation = from[Setting::short_circuit_function_evaluation];
    enable_lazy_columns_replication = from[Setting::enable_lazy_columns_replication];
}

ExpressionActionsSettings::ExpressionActionsSettings(ContextPtr from, CompileExpressions compile_expressions_)
    : ExpressionActionsSettings(from->getSettingsRef(), compile_expressions_)
{
}

}
