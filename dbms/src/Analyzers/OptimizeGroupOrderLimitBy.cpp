#include <set>
#include <unordered_set>

#include <Analyzers/OptimizeGroupOrderLimitBy.h>
#include <Analyzers/TypeAndConstantInference.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTLiteral.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


static bool isInjectiveFunction(
    const ASTFunction * ast_function,
    const TypeAndConstantInference::ExpressionInfo & function_info,
    const TypeAndConstantInference::Info & all_info)
{
    if (!function_info.function)
        return false;

    Block block_with_constants;

    const ASTs & children = ast_function->arguments->children;
    for (const auto & child : children)
    {
        String child_name = child->getColumnName();
        const TypeAndConstantInference::ExpressionInfo & child_info = all_info.at(child_name);

        block_with_constants.insert(ColumnWithTypeAndName(
            child_info.is_constant_expression ? child_info.data_type->createColumnConst(1, child_info.value) : nullptr,
            child_info.data_type,
            child_name));
    }

    return function_info.function->isInjective(block_with_constants);
}


static bool isDeterministicFunctionOfKeys(
    const ASTFunction * ast_function,
    const TypeAndConstantInference::ExpressionInfo & function_info,
    const TypeAndConstantInference::Info & all_info,
    const std::vector<std::string> & keys)
{
    if (!function_info.function || !function_info.function->isDeterministicInScopeOfQuery())
        return false;

    for (const auto & child : ast_function->arguments->children)
    {
        String child_name = child->getColumnName();
        const TypeAndConstantInference::ExpressionInfo & child_info = all_info.at(child_name);

        /// Function argument is constant.
        if (child_info.is_constant_expression)
            continue;

        /// Function argument is one of keys.
        if (keys.end() != std::find(keys.begin(), keys.end(), child_name))
            continue;

        /// Function argument is a function, that deterministically depend on keys.
        if (const ASTFunction * child_function = typeid_cast<const ASTFunction *>(child.get()))
        {
            if (isDeterministicFunctionOfKeys(child_function, child_info, all_info, keys))
                continue;
        }

        return false;
    }

    return true;
}


static void processGroupByLikeList(ASTPtr & ast, TypeAndConstantInference & expression_info)
{
    if (!ast)
        return;

    ASTs & elems = ast->children;

    std::unordered_set<std::string> unique_keys;
    size_t i = 0;

    auto restart = [&]
    {
        i = 0;
        unique_keys.clear();
    };

    /// Always leave last element in GROUP BY, even if it is constant.
    while (i < elems.size() && elems.size() > 1)
    {
        ASTPtr & elem = elems[i];

        String column_name = elem->getColumnName();                /// TODO canonicalization of names
        auto it = expression_info.info.find(column_name);
        if (it == expression_info.info.end())
            throw Exception("Type inference was not done for " + column_name, ErrorCodes::LOGICAL_ERROR);
        const TypeAndConstantInference::ExpressionInfo & info = it->second;

        /// Removing constant expressions.
        /// Removing duplicate keys.
        if (info.is_constant_expression
            || !unique_keys.emplace(column_name).second)
        {
            elems.erase(elems.begin() + i);
            continue;
        }

        if (info.function && !elem->children.empty())
        {
            const ASTFunction * ast_function = typeid_cast<const ASTFunction *>(elem.get());
            if (!ast_function)
                throw Exception("Column is marked as function during type inference, but corresponding AST node "
                    + column_name + " is not a function", ErrorCodes::LOGICAL_ERROR);

            /// Unwrap injective functions.
            if (isInjectiveFunction(ast_function, info, expression_info.info))
            {
                auto args = ast_function->arguments;
                elems.erase(elems.begin() + i);
                elems.insert(elems.begin() + i, args->children.begin(), args->children.end());

                restart();    /// Previous keys may become deterministic function of newly added keys.
                continue;
            }

            /// Remove deterministic functions of another keys.
            std::vector<String> other_keys;
            other_keys.reserve(elems.size() - 1);
            for (size_t j = 0, size = elems.size(); j < size; ++j)
                if (j != i)
                    other_keys.emplace_back(elems[j]->getColumnName());

            if (isDeterministicFunctionOfKeys(ast_function, info, expression_info.info, other_keys))
            {
                elems.erase(elems.begin() + i);
                continue;
            }
        }

        ++i;
    }
}


static void processOrderByList(ASTPtr & ast, TypeAndConstantInference & expression_info)
{
    if (!ast)
        return;

    ASTs & elems = ast->children;

    /// sort column name and collation
    std::set<std::pair<std::string, std::string>> unique_keys;
    size_t i = 0;
    while (i < elems.size())
    {
        const ASTOrderByElement * order_by_elem = typeid_cast<const ASTOrderByElement *>(elems[i].get());
        if (!order_by_elem)
            throw Exception("Child of ORDER BY clause is not an ASTOrderByElement", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        /// It has ASC|DESC and COLLATE inplace, and expression as its only child.
        if (order_by_elem->children.empty())
            throw Exception("ORDER BY element has no children", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        const ASTPtr & elem = order_by_elem->children[0];
        String collation;
        if (order_by_elem->collation)
        {
            const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(order_by_elem->collation.get());
            if (!lit)
                throw Exception("Collation in ORDER BY clause is not an ASTLiteral", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            if (lit->value.getType() != Field::Types::String)
                throw Exception("Collation in ORDER BY clause is not a string literal", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

            collation = lit->value.get<String>();
        }

        String column_name = elem->getColumnName();
        auto it = expression_info.info.find(column_name);
        if (it == expression_info.info.end())
            throw Exception("Type inference was not done for " + column_name, ErrorCodes::LOGICAL_ERROR);
        const TypeAndConstantInference::ExpressionInfo & info = it->second;

        /// Removing constant expressions.
        /// Removing duplicate keys.
        if (info.is_constant_expression
            /// Having same element but with empty collation. Empty collation is considered more "granular" than any special collation.
            || unique_keys.count(std::make_pair(column_name, String()))
            /// Having same element with same collation.
            || !unique_keys.emplace(column_name, collation).second)
        {
            elems.erase(elems.begin() + i);
            continue;
        }

        if (i > 0 && collation.empty() && info.function && !elem->children.empty())
        {
            const ASTFunction * ast_function = typeid_cast<const ASTFunction *>(elem.get());
            if (!ast_function)
                throw Exception("Column is marked as function during type inference, but corresponding AST node "
                    + column_name + " is not a function", ErrorCodes::LOGICAL_ERROR);

            /// Remove deterministic functions of previous keys. Only consider keys without collation.
            std::vector<String> prev_keys;
            prev_keys.reserve(i);
            for (size_t j = 0; j < i; ++j)
                if (!typeid_cast<const ASTOrderByElement &>(*elems[j]).collation)
                    prev_keys.emplace_back(elems[j]->children.at(0)->getColumnName());

            if (isDeterministicFunctionOfKeys(ast_function, info, expression_info.info, prev_keys))
            {
                elems.erase(elems.begin() + i);
                continue;
            }
        }

        ++i;
    }
}


void OptimizeGroupOrderLimitBy::process(ASTPtr & ast, TypeAndConstantInference & expression_info)
{
    ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get());
    if (!select)
        throw Exception("AnalyzeResultOfQuery::process was called for not a SELECT query", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    if (!select->select_expression_list)
        throw Exception("SELECT query doesn't have select_expression_list", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    processGroupByLikeList(select->group_expression_list, expression_info);
    processGroupByLikeList(select->limit_by_expression_list, expression_info);

    if (select->order_expression_list)
    {
        processOrderByList(select->order_expression_list, expression_info);

        /// ORDER BY could be completely eliminated
        if (select->order_expression_list->children.empty())
        {
            select->children.erase(std::remove(
                select->children.begin(), select->children.end(), select->order_expression_list), select->children.end());
            select->order_expression_list.reset();
        }
    }
}


}
