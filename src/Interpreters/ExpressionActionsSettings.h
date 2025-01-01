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
    ExpressionActionsSettings(); /// TODO Remove this constructor. ExpressionActionsSettings should only be initializable from Settings or
                                 /// ContextPtr (i.e. the other two constructors).
    explicit ExpressionActionsSettings(const Settings & from, CompileExpressions compile_expressions_ = CompileExpressions::no);
    explicit ExpressionActionsSettings(ContextPtr from, CompileExpressions compile_expressions_ = CompileExpressions::no);

    bool can_compile_expressions;
    size_t min_count_to_compile_expression;

    size_t max_temporary_columns;
    size_t max_temporary_non_const_columns;

    CompileExpressions compile_expressions;

    ShortCircuitFunctionEvaluation short_circuit_function_evaluation;
};

}
