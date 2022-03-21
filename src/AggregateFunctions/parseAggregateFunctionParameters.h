#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

Array getAggregateFunctionParametersArray(
    const ASTPtr & expression_list,
    const std::string & error_context,
    ContextPtr context);

void getAggregateFunctionNameAndParametersArray(
    const std::string & aggregate_function_name_with_params,
    std::string & aggregate_function_name,
    Array & aggregate_function_parameters,
    const std::string & error_context,
    ContextPtr context);

}
