#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTHelpers.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTSubquery.h>
#include <Analyzer/Resolve/ScopeAliases.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionParameterValuesVisitor
{
public:
    explicit FunctionParameterValuesVisitor(NameToNameMap & parameter_values_, const ScopeAliases & aliases_)
        : parameter_values(parameter_values_), aliases(aliases_)
    {
    }

    void visit(const ASTPtr & ast)
    {
        if (const auto * function = ast->as<ASTFunction>())
            visitFunction(*function);

        for (const auto & child : ast->children)
            visit(child);
    }

private:
    NameToNameMap & parameter_values;
    const ScopeAliases & aliases;

    std::string tryGetParameterValueAsString(const ASTPtr & ast)
    {
        if (const auto * literal = ast->as<ASTLiteral>())
        {
            return convertFieldToString(literal->value);
        }
        else if (const auto * value_identifier = ast->as<ASTIdentifier>())
        {
            auto it = aliases.alias_name_to_expression_node_before_group_by.find(value_identifier->name());
            if (it != aliases.alias_name_to_expression_node_before_group_by.end())
            {
                return tryGetParameterValueAsString(it->second->toAST());
            }
        }
        else if (const auto * function = ast->as<ASTFunction>())
        {
            if (isFunctionCast(function))
            {
                const auto * cast_expression = assert_cast<ASTExpressionList*>(function->arguments.get());
                if (cast_expression->children.size() != 2)
                    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function CAST must have exactly two arguments");

                if (const auto * cast_literal = cast_expression->children[0]->as<ASTLiteral>())
                {
                    return convertFieldToString(cast_literal->value);
                }
            }
        }
        return "";
    }

    void visitFunction(const ASTFunction & parameter_function)
    {
        if (parameter_function.name != "equals" && parameter_function.children.size() != 1)
            return;

        const auto * expression_list = parameter_function.children[0]->as<ASTExpressionList>();

        if (expression_list && expression_list->children.size() != 2)
            return;

        if (const auto * identifier = expression_list->children[0]->as<ASTIdentifier>())
        {
            auto value_str = tryGetParameterValueAsString(expression_list->children[1]);
            if (!value_str.empty())
                parameter_values[identifier->name()] = value_str;
        }
    }
};

NameToNameMap analyzeFunctionParamValues(const ASTPtr & ast, const ScopeAliases & scope_aliases)
{
    NameToNameMap parameter_values;
    FunctionParameterValuesVisitor(parameter_values, scope_aliases).visit(ast);
    return parameter_values;
}


}
