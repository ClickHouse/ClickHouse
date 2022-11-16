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
        if (const auto * expression = ast->as<ASTExpressionList>())
            visitExpressionList(*expression);
        for (const auto & child : ast->children)
            visit(child);
    }

private:
    NameToNameMap & parameter_values;

    void visitExpressionList(const ASTExpressionList & expression_list)
    {
        if (expression_list.children.size() != 2)
            return;

        if (const auto * identifier = expression_list.children[0]->as<ASTIdentifier>())
        {
            if (const auto * literal = expression_list.children[1]->as<ASTLiteral>())
            {
                parameter_values[identifier->name()] = convertFieldToString(literal->value);
            }
            else if (const auto * function = expression_list.children[1]->as<ASTFunction>())
            {
                if (isFunctionCast(function))
                {
                    const auto * cast_expression = assert_cast<ASTExpressionList*>(function->arguments.get());
                    if (cast_expression->children.size() != 2)
                        throw Exception("Function CAST must have exactly two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
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
