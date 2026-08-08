#include <unordered_set>

#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/ArithmeticOperationsInAgrFuncOptimize.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

namespace
{

const ASTFunction * getInternalFunction(const ASTFunction & func)
{
    if (func.arguments && func.arguments->children.size() == 1)
        return func.arguments->children[0]->as<ASTFunction>();
    return nullptr;
}

ASTPtr exchangeExtractFirstArgument(const String & func_name, const ASTFunction & child_func)
{
    ASTs new_child_args;
    new_child_args.push_back(child_func.arguments->children[1]);

    auto new_child = makeASTFunction(func_name, new_child_args);

    ASTs new_args;
    new_args.push_back(child_func.arguments->children[0]);
    new_args.push_back(new_child);

    return makeASTFunction(child_func.name, new_args);
}

ASTPtr exchangeExtractSecondArgument(const String & func_name, const ASTFunction & child_func)
{
    ASTs new_child_args;
    new_child_args.push_back(child_func.arguments->children[0]);

    auto new_child = makeASTFunction(func_name, new_child_args);

    ASTs new_args;
    new_args.push_back(new_child);
    new_args.push_back(child_func.arguments->children[1]);

    return makeASTFunction(child_func.name, new_args);
}

Field zeroField(const Field & value)
{
    switch (value.getType())
    {
        case Field::Types::UInt64: return static_cast<UInt64>(0);
        case Field::Types::Int64: return static_cast<Int64>(0);
        case Field::Types::Float64: return static_cast<Float64>(0);
        case Field::Types::UInt128: return static_cast<UInt128>(0);
        case Field::Types::Int128: return static_cast<Int128>(0);
        case Field::Types::UInt256: return static_cast<UInt256>(0);
        case Field::Types::Int256: return static_cast<Int256>(0);
        default:
            break;
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Unexpected literal type in function");
}

ASTPtr tryExchangeFunctions(const ASTFunction & func)
{
    static const std::unordered_map<String, std::unordered_set<String>> supported
        = {{"sum", {"multiply", "divide"}},
           {"min", {"multiply", "divide", "plus", "minus"}},
           {"max", {"multiply", "divide", "plus", "minus"}},
           {"avg", {"multiply", "divide", "plus", "minus"}}};

    /// Aggregate functions[sum|min|max|avg] is case-insensitive, so we use lower cases name
    auto lower_name = Poco::toLower(func.name);

    const ASTFunction * child_func = getInternalFunction(func);
    if (!child_func || !child_func->arguments || child_func->arguments->children.size() != 2 || !supported.contains(lower_name)
        || !supported.find(lower_name)->second.contains(child_func->name))
        return {};

    auto original_alias = func.tryGetAlias();
    const auto & child_func_args = child_func->arguments->children;
    const auto * first_literal = child_func_args[0]->as<ASTLiteral>();
    const auto * second_literal = child_func_args[1]->as<ASTLiteral>();

    ASTPtr optimized_ast;

    /** Need reverse max <-> min for:
      *
      * max(-1*value) -> -1*min(value)
      * max(value/-2) -> min(value)/-2
      * max(1-value) -> 1-min(value)
      */
    auto get_reverse_aggregate_function_name = [](const std::string & aggregate_function_name) -> std::string
    {
        if (aggregate_function_name == "min")
            return "max";
        if (aggregate_function_name == "max")
            return "min";
        return aggregate_function_name;
    };

    if (first_literal && !second_literal)
    {
        /// It's possible to rewrite 'sum(1/n)' with 'sum(1) * div(1/n)' but we lose accuracy. Ignored.
        if (child_func->name == "divide")
            return {};
        bool need_reverse
            = (child_func->name == "multiply" && first_literal->value < zeroField(first_literal->value)) || child_func->name == "minus";
        if (need_reverse)
            lower_name = get_reverse_aggregate_function_name(lower_name);

        optimized_ast = exchangeExtractFirstArgument(lower_name, *child_func);
    }
    else if (second_literal) /// second or both are consts
    {
        bool need_reverse
            = (child_func->name == "multiply" || child_func->name == "divide") && second_literal->value < zeroField(second_literal->value);
        if (need_reverse)
            lower_name = get_reverse_aggregate_function_name(lower_name);

        optimized_ast = exchangeExtractSecondArgument(lower_name, *child_func);
    }

    if (optimized_ast)
    {
        optimized_ast->setAlias(original_alias);
        return optimized_ast;
    }
    return {};
}

}


void ArithmeticOperationsInAgrFuncMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data & data)
{
    if (auto exchanged_funcs = tryExchangeFunctions(func))
    {
        ast = exchanged_funcs;

        /// Main visitor is bottom-up. This is top-down part.
        /// We've found an aggregate function an now move it down through others: sum(mul(mul)) -> mul(mul(sum)).
        /// It's not dangerous cause main visitor already has visited this part of tree.
        auto & expression_list = ast->children[0];
        visit(expression_list->children[0], data);
    }
}

void ArithmeticOperationsInAgrFuncMatcher::visit(ASTPtr & ast, Data & data)
{
    if (const auto * function_node = ast->as<ASTFunction>())
    {
        if (function_node->is_window_function)
            return;

        visit(*function_node, ast, data);
    }
}

bool ArithmeticOperationsInAgrFuncMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>() &&
        !node->as<ASTTableExpression>() &&
        !node->as<ASTArrayJoin>();
}

}
