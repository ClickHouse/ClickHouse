#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/RewriteSumIfFunctionVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>

namespace DB
{

void RewriteSumIfFunctionMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
    {
        if (func->is_window_function)
            return;

        visit(*func, ast, data);
    }
}

void RewriteSumIfFunctionMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data &)
{
    if (!func.arguments || func.arguments->children.empty())
        return;

    auto lower_name = Poco::toLower(func.name);

    /// sumIf, SumIf or sUMIf are valid function names, but sumIF or sumiF are not
    if (lower_name != "sum" && (lower_name != "sumif" || !endsWith(func.name, "If")))
        return;

    const auto & func_arguments = func.arguments->children;

    if (lower_name == "sumif")
    {
        const auto * literal = func_arguments[0]->as<ASTLiteral>();
        if (!literal || !DB::isInt64OrUInt64FieldType(literal->value.getType()))
            return;

        if (func_arguments.size() == 2)
        {
            std::shared_ptr<ASTFunction> new_func;
            if (literal->value.get<UInt64>() == 1)
            {
                /// sumIf(1, cond) -> countIf(cond)
                new_func = makeASTFunction("countIf", func_arguments[1]);
            }
            else
            {
                /// sumIf(123, cond) -> 123 * countIf(cond)
                auto count_if_func = makeASTFunction("countIf", func_arguments[1]);
                new_func = makeASTFunction("multiply", func_arguments[0], std::move(count_if_func));
            }
            new_func->setAlias(func.alias);
            ast = std::move(new_func);
            return;
        }
    }
    else
    {
        const auto * nested_func = func_arguments[0]->as<ASTFunction>();

        if (!nested_func || Poco::toLower(nested_func->name) != "if" || nested_func->arguments->children.size() != 3)
            return;

        const auto & if_arguments = nested_func->arguments->children;

        const auto * first_literal = if_arguments[1]->as<ASTLiteral>();
        const auto * second_literal = if_arguments[2]->as<ASTLiteral>();

        if (first_literal && second_literal)
        {
            if (!DB::isInt64OrUInt64FieldType(first_literal->value.getType()) || !DB::isInt64OrUInt64FieldType(second_literal->value.getType()))
                return;

            auto first_value = first_literal->value.get<UInt64>();
            auto second_value = second_literal->value.get<UInt64>();

            std::shared_ptr<ASTFunction> new_func;
            if (second_value == 0)
            {
                if (first_value == 1)
                {
                    /// sum(if(cond, 1, 0)) -> countIf(cond)
                    new_func = makeASTFunction("countIf", if_arguments[0]);
                }
                else
                {
                    /// sum(if(cond, 123, 0)) -> 123 * countIf(cond)
                    auto count_if_func = makeASTFunction("countIf", if_arguments[0]);
                    new_func = makeASTFunction("multiply", if_arguments[1], std::move(count_if_func));
                }
                new_func->setAlias(func.alias);
                ast = std::move(new_func);
                return;
            }

            if (first_value == 0)
            {
                auto not_func = makeASTFunction("not", if_arguments[0]);
                if (second_value == 1)
                {
                    /// sum(if(cond, 0, 1)) -> countIf(not(cond))
                    new_func = makeASTFunction("countIf", std::move(not_func));
                }
                else
                {
                    /// sum(if(cond, 0, 123)) -> 123 * countIf(not(cond))
                    auto count_if_func = makeASTFunction("countIf", std::move(not_func));
                    new_func = makeASTFunction("multiply", if_arguments[2], std::move(count_if_func));
                }
                new_func->setAlias(func.alias);
                ast = std::move(new_func);
                return;
            }
        }
    }

}

}
