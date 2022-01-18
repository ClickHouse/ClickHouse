#pragma once
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>


namespace DB
{

Array getAggregateFunctionParametersArray(const ASTPtr & expression_list, const std::string & error_context = "");


void getAggregateFunctionNameAndParametersArray(
    const std::string & aggregate_function_name_with_params,
    std::string & aggregate_function_name,
    Array & aggregate_function_parameters,
    const std::string & error_context);

}
