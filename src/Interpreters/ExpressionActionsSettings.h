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
    ExpressionActionsSettings() = default;
    explicit ExpressionActionsSettings(const Settings & from, CompileExpressions compile_expressions_ = CompileExpressions::no);
    explicit ExpressionActionsSettings(ContextPtr from, CompileExpressions compile_expressions_ = CompileExpressions::no);

    bool can_compile_expressions = false;
    size_t min_count_to_compile_expression = 0;

    size_t max_temporary_columns = 0;
    size_t max_temporary_non_const_columns = 0;

    CompileExpressions compile_expressions = CompileExpressions::no;

    ShortCircuitFunctionEvaluation short_circuit_function_evaluation = ShortCircuitFunctionEvaluation::DISABLE;

    bool enable_lazy_columns_replication = false;
};

}
