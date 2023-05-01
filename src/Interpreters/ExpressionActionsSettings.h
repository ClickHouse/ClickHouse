#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/SettingsEnums.h>

#include <cstddef>

namespace DB
{

struct Settings;

enum class CompileExpressions: uint8_t
{
    no = 0,
    yes = 1,
};

struct ExpressionActionsSettings
{
    bool can_compile_expressions = false;
    size_t min_count_to_compile_expression = 0;

    size_t max_temporary_columns = 0;
    size_t max_temporary_non_const_columns = 0;

    CompileExpressions compile_expressions = CompileExpressions::no;

    ShortCircuitFunctionEvaluation short_circuit_function_evaluation = ShortCircuitFunctionEvaluation::DISABLE;

    static ExpressionActionsSettings fromSettings(const Settings & from, CompileExpressions compile_expressions = CompileExpressions::no);
    static ExpressionActionsSettings fromContext(ContextPtr from, CompileExpressions compile_expressions = CompileExpressions::no);
};

}
