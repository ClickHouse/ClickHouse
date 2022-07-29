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
        return func.arguments->children.front()->as<ASTFunction>();
    return nullptr;
}

static ASTPtr exchangeExtractFirstArgument(const String & func_name, const ASTFunction & child_func)
{
    ASTList new_child_args;
    new_child_args.push_back(child_func.arguments->children.back());

    auto new_child = makeASTFunction(func_name, new_child_args);

    ASTList new_args;
    new_args.push_back(child_func.arguments->children.front());
    new_args.push_back(new_child);

    return makeASTFunction(child_func.name, new_args);
}

static ASTPtr exchangeExtractSecondArgument(const String & func_name, const ASTFunction & child_func)
{
    ASTList new_child_args;
    new_child_args.push_back(child_func.arguments->children.front());

    auto new_child = makeASTFunction(func_name, new_child_args);

    ASTList new_args;
    new_args.push_back(new_child);
    new_args.push_back(child_func.arguments->children.back());

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

    throw Exception("Unexpected literal type in function", ErrorCodes::BAD_TYPE_OF_FIELD);
}

const String & changeNameIfNeeded(const String & func_name, const String & child_name, const ASTLiteral & literal)
{
    static const std::unordered_map<String, std::unordered_set<String>> matches = {
        { "min", { "multiply", "divide" } },
        { "max", { "multiply", "divide" } }
    };

    static const std::unordered_map<String, String> swap_to = {
        { "min", "max" },
        { "max", "min" }
    };

    if (literal.value < zeroField(literal.value) && matches.contains(func_name) && matches.find(func_name)->second.contains(child_name))
        return swap_to.find(func_name)->second;

    return func_name;
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
    const auto * first_literal = child_func_args.front()->as<ASTLiteral>();
    const auto * second_literal = child_func_args.back()->as<ASTLiteral>();

    ASTPtr optimized_ast;

    if (first_literal && !second_literal)
    {
        /// It's possible to rewrite 'sum(1/n)' with 'sum(1) * div(1/n)' but we lose accuracy. Ignored.
        if (child_func->name == "divide")
            return {};

        const String & new_name = changeNameIfNeeded(lower_name, child_func->name, *first_literal);
        optimized_ast = exchangeExtractFirstArgument(new_name, *child_func);
    }
    else if (second_literal) /// second or both are consts
    {
        const String & new_name = changeNameIfNeeded(lower_name, child_func->name, *second_literal);
        optimized_ast = exchangeExtractSecondArgument(new_name, *child_func);
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
        auto & expression_list = ast->children.front();
        visit(expression_list->children.front(), data);
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
