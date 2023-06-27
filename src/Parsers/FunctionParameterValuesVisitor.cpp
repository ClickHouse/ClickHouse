#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Common/FieldVisitorToString.h>
#include <Parsers/ASTHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionParameterValuesVisitor
{
public:
    explicit FunctionParameterValuesVisitor(NameToNameMap & parameter_values_)
        : parameter_values(parameter_values_)
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

    void visitFunction(const ASTFunction & parameter_function)
    {
        if (parameter_function.name != "equals" && parameter_function.children.size() != 1)
            return;

        const auto * expression_list = parameter_function.children[0]->as<ASTExpressionList>();

        if (expression_list && expression_list->children.size() != 2)
            return;

        if (const auto * identifier = expression_list->children[0]->as<ASTIdentifier>())
        {
            if (const auto * literal = expression_list->children[1]->as<ASTLiteral>())
            {
                parameter_values[identifier->name()] = convertFieldToString(literal->value);
            }
            else if (const auto * function = expression_list->children[1]->as<ASTFunction>())
            {
                if (isFunctionCast(function))
                {
                    const auto * cast_expression = assert_cast<ASTExpressionList*>(function->arguments.get());
                    if (cast_expression->children.size() != 2)
                        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function CAST must have exactly two arguments");
                    if (const auto * cast_literal = cast_expression->children[0]->as<ASTLiteral>())
                    {
                        parameter_values[identifier->name()] = convertFieldToString(cast_literal->value);
                    }
                }
            }
        }
    }
};

NameToNameMap analyzeFunctionParamValues(const ASTPtr & ast)
{
    NameToNameMap parameter_values;
    FunctionParameterValuesVisitor(parameter_values).visit(ast);
    return parameter_values;
}


}
