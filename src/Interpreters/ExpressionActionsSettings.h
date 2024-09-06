#pragma once

#include <Core/ShortCircuitFunctionEvaluation.h>
#include <Interpreters/Context_fwd.h>

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
    size_t min_count_to_compile_expression_v = 0;

    size_t max_temporary_columns_v = 0;
    size_t max_temporary_non_const_columns_v = 0;

    CompileExpressions compile_expressions_v = CompileExpressions::no;

    ShortCircuitFunctionEvaluation short_circuit_function_evaluation_v = ShortCircuitFunctionEvaluation::DISABLE;

    static ExpressionActionsSettings fromSettings(const Settings & from, CompileExpressions compile_expressions_v = CompileExpressions::no);
    static ExpressionActionsSettings fromContext(ContextPtr from, CompileExpressions compile_expressions_v = CompileExpressions::no);
};

}
