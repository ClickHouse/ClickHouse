#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/RewriteAggregateFunctionWithIfVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>

namespace DB
{

void RewriteAggregateFunctionWithIfMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * func = ast->as<ASTFunction>())
    {
        if (func->is_window_function)
            return;

        visit(*func, ast, data);
    }
}

void RewriteAggregateFunctionWithIfMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data &)
{
    const auto & factory = AggregateFunctionFactory::instance();
    if (!factory.isAggregateFunctionName(func.name))
        return;

    if (!func.arguments || func.arguments->children.size() != 1)
        return;

    auto * if_func = func.arguments->children.back()->as<ASTFunction>();
    if (!if_func || Poco::toLower(if_func->name) != "if")
        return;

    auto lower_name = Poco::toLower(func.name);
    const auto & if_arguments = if_func->arguments->children;
    const auto * first_literal = if_arguments[1]->as<ASTLiteral>();
    const auto * second_literal = if_arguments[2]->as<ASTLiteral>();
    if (second_literal)
    {
        if (second_literal->value.isNull()
            || (lower_name == "sum" && isInt64OrUInt64FieldType(second_literal->value.getType())
                && second_literal->value.get<UInt64>() == 0))
        {
            /// avg(if(cond, a, null)) -> avgIf(a, cond)
            /// sum(if(cond, a, 0)) -> sumIf(a, cond)
            auto new_func
                = makeASTFunction(func.name + (second_literal->value.isNull() ? "IfOrNull" : "If"), if_arguments[1], if_arguments[0]);
            new_func->setAlias(func.alias);
            new_func->parameters = func.parameters;

            ast = std::move(new_func);
            return;
        }
    }
    else if (first_literal)
    {
        if (first_literal->value.isNull()
            || (lower_name == "sum" && isInt64OrUInt64FieldType(first_literal->value.getType()) && first_literal->value.get<UInt64>() == 0))
        {
            /// avg(if(cond, null, a) -> avgIf(a, !cond))
            /// sum(if(cond, 0, a) -> sumIf(a, !cond))
            auto not_func = makeASTFunction("not", if_arguments[0]);
            auto new_func
                = makeASTFunction(func.name + (first_literal->value.isNull() ? "IfOrNull" : "If"), if_arguments[2], std::move(not_func));
            new_func->setAlias(func.alias);
            new_func->parameters = func.parameters;

            ast = std::move(new_func);
            return;
        }
    }
}

}
