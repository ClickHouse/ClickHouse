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
        case Field::Types::UInt64: return UInt64(0);
        case Field::Types::Int64: return Int64(0);
        case Field::Types::Float64: return Float64(0);
        case Field::Types::UInt128: return UInt128(0);
        case Field::Types::Int128: return Int128(0);
        case Field::Types::UInt256: return UInt256(0);
        case Field::Types::Int256: return Int256(0);
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

    if (literal.value < zeroField(literal.value) && matches.count(func_name) && matches.find(func_name)->second.count(child_name))
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
    if (!child_func || !child_func->arguments || child_func->arguments->children.size() != 2 || !supported.count(lower_name)
        || !supported.find(lower_name)->second.count(child_func->name))
        return {};

    /// Cannot rewrite function with alias cause alias could become undefined
    if (!func.tryGetAlias().empty() || !child_func->tryGetAlias().empty())
        return {};

    const auto & child_func_args = child_func->arguments->children;
    const auto * first_literal = child_func_args[0]->as<ASTLiteral>();
    const auto * second_literal = child_func_args[1]->as<ASTLiteral>();

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

    return optimized_ast;
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
        visit(*function_node, ast, data);
}

bool ArithmeticOperationsInAgrFuncMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>() &&
        !node->as<ASTTableExpression>() &&
        !node->as<ASTArrayJoin>();
}

}
