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
        visit(*func, ast, data);
}

void RewriteSumIfFunctionMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data &)
{
    if (!func.arguments || func.arguments->children.empty())
        return;

    auto lower_name = Poco::toLower(func.name);

    if (lower_name != "sum" && lower_name != "sumif")
        return;

    const auto & func_arguments = func.arguments->children;

    if (lower_name == "sumif")
    {
        /// sumIf(1, cond) -> countIf(cond)
        const auto * literal = func_arguments[0]->as<ASTLiteral>();
        if (!literal || !DB::isInt64FieldType(literal->value.getType()))
            return;

        if (func_arguments.size() == 2 && literal->value.get<UInt64>() == 1)
        {
            auto new_func = makeASTFunction("countIf", func_arguments[1]);
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
            if (!DB::isInt64FieldType(first_literal->value.getType()) || !DB::isInt64FieldType(second_literal->value.getType()))
                return;

            auto first_value = first_literal->value.get<UInt64>();
            auto second_value = second_literal->value.get<UInt64>();
            /// sum(if(cond, 1, 0)) -> countIf(cond)
            if (first_value == 1 && second_value == 0)
            {
                auto new_func = makeASTFunction("countIf", if_arguments[0]);
                new_func->setAlias(func.alias);
                ast = std::move(new_func);
                return;
            }
            /// sum(if(cond, 0, 1)) -> countIf(not(cond))
            if (first_value == 0 && second_value == 1)
            {
                auto not_func = makeASTFunction("not", if_arguments[0]);
                auto new_func = makeASTFunction("countIf", not_func);
                new_func->setAlias(func.alias);
                ast = std::move(new_func);
                return;
            }
        }
    }

}

}
