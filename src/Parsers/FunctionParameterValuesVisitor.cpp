#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTHelpers.h>
#include <Analyzer/Resolve/ScopeAliases.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionParameterValuesVisitor
{
public:
    explicit FunctionParameterValuesVisitor(
        ParamValuesAnalyzeResult & result_,
        ContextPtr context_,
        const ScopeAliases * aliases_)
        : result(result_)
        , aliases(aliases_)
        , context(context_)
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
    ParamValuesAnalyzeResult & result;
    const ScopeAliases * aliases;
    ContextPtr context;

    std::optional<std::string> tryGetParameterValueAsString(const std::string & param_name, const ASTPtr & ast)
    {
        if (const auto * literal = ast->as<ASTLiteral>())
        {
            return convertFieldToString(literal->value);
        }
        else if (const auto * value_identifier = ast->as<ASTIdentifier>())
        {
            if (aliases)
            {
                auto it = aliases->alias_name_to_expression_node_before_group_by.find(value_identifier->name());
                if (it != aliases->alias_name_to_expression_node_before_group_by.end())
                {
                    auto value_str = tryGetParameterValueAsString(param_name, it->second->toAST());
                    if (!value_str.has_value())
                        result.unresolved_param_aliases.emplace(param_name, value_identifier->name());
                    return value_str;
                }
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
            else
            {
                ASTPtr res = evaluateConstantExpressionOrIdentifierAsLiteral(ast, context);
                return convertFieldToString(res->as<ASTLiteral>()->value);
            }
        }
        return std::nullopt;
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
            auto value_str = tryGetParameterValueAsString(identifier->name(), expression_list->children[1]);
            if (value_str.has_value())
                result.resolved_param_values[identifier->name()] = value_str.value();
        }
    }
};

ParamValuesAnalyzeResult analyzeFunctionParamValues(const ASTPtr & ast, const ContextPtr & context, const ScopeAliases * scope_aliases)
{
    ParamValuesAnalyzeResult result;
    FunctionParameterValuesVisitor(result, context, scope_aliases).visit(ast);
    return result;
}


}
