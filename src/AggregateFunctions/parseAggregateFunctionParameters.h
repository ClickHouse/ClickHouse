#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct Array;

/// Parameters are evaluated on every call. `parameters_are_deterministic`, if passed, is set to false when a parameter
/// may give another value next time (`randConstant()`), see `evaluateConstantExpression`.
Array getAggregateFunctionParametersArray(
    const ASTPtr & expression_list,
    const std::string & error_context,
    ContextPtr context,
    bool * parameters_are_deterministic = nullptr);

void getAggregateFunctionNameAndParametersArray(
    const std::string & aggregate_function_name_with_params,
    std::string & aggregate_function_name,
    Array & aggregate_function_parameters,
    const std::string & error_context,
    ContextPtr context,
    bool * parameters_are_deterministic = nullptr);

}
