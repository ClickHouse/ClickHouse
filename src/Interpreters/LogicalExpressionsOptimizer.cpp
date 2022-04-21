#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Core/Settings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>

#include <deque>

#include <base/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


LogicalExpressionsOptimizer::OrWithExpression::OrWithExpression(const ASTFunction * or_function_,
    const IAST::Hash & expression_, const std::string & alias_)
    : or_function(or_function_), expression(expression_), alias(alias_)
{
}

bool LogicalExpressionsOptimizer::OrWithExpression::operator<(const OrWithExpression & rhs) const
{
    return std::tie(this->or_function, this->expression) < std::tie(rhs.or_function, rhs.expression);
}

LogicalExpressionsOptimizer::LogicalExpressionsOptimizer(ASTSelectQuery * select_query_, UInt64 optimize_min_equality_disjunction_chain_length)
    : select_query(select_query_), settings(optimize_min_equality_disjunction_chain_length)
{
}

void LogicalExpressionsOptimizer::perform()
{
    if (select_query == nullptr)
        return;
    if (visited_nodes.contains(select_query))
        return;

    size_t position = 0;
    for (auto & column : select_query->select()->children)
    {
        bool inserted = column_to_position.emplace(column.get(), position).second;

        /// Do not run, if AST was already converted to DAG.
        /// TODO This is temporary solution. We must completely eliminate conversion of AST to DAG.
        /// (see ExpressionAnalyzer::normalizeTree)
        if (!inserted)
            return;

        ++position;
    }

    collectDisjunctiveEqualityChains();

    for (auto & chain : disjunctive_equality_chains_map)
    {
        if (!mayOptimizeDisjunctiveEqualityChain(chain))
            continue;
        addInExpression(chain);

        auto & equalities = chain.second;
        equalities.is_processed = true;
        ++processed_count;
    }

    if (processed_count > 0)
    {
        cleanupOrExpressions();
        fixBrokenOrExpressions();
        reorderColumns();
    }
}

void LogicalExpressionsOptimizer::reorderColumns()
{
    auto & columns = select_query->select()->children;
    size_t cur_position = 0;

    while (cur_position < columns.size())
    {
        size_t expected_position = column_to_position.at(columns[cur_position].get());
        if (cur_position != expected_position)
            std::swap(columns[cur_position], columns[expected_position]);
        else
            ++cur_position;
    }
}

void LogicalExpressionsOptimizer::collectDisjunctiveEqualityChains()
{
    if (visited_nodes.contains(select_query))
        return;

    using Edge = std::pair<IAST *, IAST *>;
    std::deque<Edge> to_visit;

    to_visit.emplace_back(nullptr, select_query);
    while (!to_visit.empty())
    {
        auto edge = to_visit.back();
        auto * from_node = edge.first;
        auto * to_node = edge.second;

        to_visit.pop_back();

        bool found_chain = false;

        auto * function = to_node->as<ASTFunction>();
        if (function && function->name == "or" && function->children.size() == 1)
        {
            const auto * expression_list = function->children[0]->as<ASTExpressionList>();
            if (expression_list)
            {
                /// The chain of elements of the OR expression.
                for (const auto & child : expression_list->children)
                {
                    auto * equals = child->as<ASTFunction>();
                    if (equals && equals->name == "equals" && equals->children.size() == 1)
                    {
                        const auto * equals_expression_list = equals->children[0]->as<ASTExpressionList>();
                        if (equals_expression_list && equals_expression_list->children.size() == 2)
                        {
                            /// Equality expr = xN.
                            const auto * literal = equals_expression_list->children[1]->as<ASTLiteral>();
                            if (literal)
                            {
                                auto expr_lhs = equals_expression_list->children[0]->getTreeHash();
                                OrWithExpression or_with_expression{function, expr_lhs, function->tryGetAlias()};
                                disjunctive_equality_chains_map[or_with_expression].functions.push_back(equals);
                                found_chain = true;
                            }
                        }
                    }
                }
            }
        }

        visited_nodes.insert(to_node);

        if (found_chain)
        {
            if (from_node != nullptr)
            {
                auto res = or_parent_map.insert(std::make_pair(function, ParentNodes{from_node}));
                if (!res.second)
                    throw Exception("LogicalExpressionsOptimizer: parent node information is corrupted",
                        ErrorCodes::LOGICAL_ERROR);
            }
        }
        else
        {
            for (auto & child : to_node->children)
            {
                if (!child->as<ASTSelectQuery>())
                {
                    if (!visited_nodes.contains(child.get()))
                        to_visit.push_back(Edge(to_node, &*child));
                    else
                    {
                        /// If the node is an OR function, update the information about its parents.
                        auto it = or_parent_map.find(&*child);
                        if (it != or_parent_map.end())
                        {
                            auto & parent_nodes = it->second;
                            parent_nodes.push_back(to_node);
                        }
                    }
                }
            }
        }
    }

    for (auto & chain : disjunctive_equality_chains_map)
    {
        auto & equalities = chain.second;
        auto & equality_functions = equalities.functions;
        ::sort(equality_functions.begin(), equality_functions.end());
    }
}

namespace
{

inline ASTs & getFunctionOperands(const ASTFunction * or_function)
{
    return or_function->children[0]->children;
}

}

bool LogicalExpressionsOptimizer::mayOptimizeDisjunctiveEqualityChain(const DisjunctiveEqualityChain & chain) const
{
    const auto & equalities = chain.second;
    const auto & equality_functions = equalities.functions;

    /// We eliminate too short chains.
    if (equality_functions.size() < settings.optimize_min_equality_disjunction_chain_length)
        return false;

    /// We check that the right-hand sides of all equalities have the same type.
    auto & first_operands = getFunctionOperands(equality_functions[0]);
    const auto * first_literal = first_operands[1]->as<ASTLiteral>();
    for (size_t i = 1; i < equality_functions.size(); ++i)
    {
        auto & operands = getFunctionOperands(equality_functions[i]);
        const auto * literal = operands[1]->as<ASTLiteral>();

        if (literal->value.getType() != first_literal->value.getType())
            return false;
    }
    return true;
}

void LogicalExpressionsOptimizer::addInExpression(const DisjunctiveEqualityChain & chain)
{
    const auto & or_with_expression = chain.first;
    const auto & equalities = chain.second;
    const auto & equality_functions = equalities.functions;

    /// 1. Create a new IN expression based on information from the OR-chain.

    /// Construct a tuple of literals `x1, ..., xN` from the string `expr = x1 OR ... OR expr = xN`

    Tuple tuple;
    tuple.reserve(equality_functions.size());

    for (const auto * function : equality_functions)
    {
        const auto & operands = getFunctionOperands(function);
        tuple.push_back(operands[1]->as<ASTLiteral>()->value);
    }

    /// Sort the literals so that they are specified in the same order in the IN expression.
    ::sort(tuple.begin(), tuple.end());

    /// Get the expression `expr` from the chain `expr = x1 OR ... OR expr = xN`
    ASTPtr equals_expr_lhs;
    {
        auto * function = equality_functions[0];
        const auto & operands = getFunctionOperands(function);
        equals_expr_lhs = operands[0];
    }

    auto tuple_literal = std::make_shared<ASTLiteral>(std::move(tuple));

    ASTPtr expression_list = std::make_shared<ASTExpressionList>();
    expression_list->children.push_back(equals_expr_lhs);
    expression_list->children.push_back(tuple_literal);

    /// Construct the expression `expr IN (x1, ..., xN)`
    auto in_function = std::make_shared<ASTFunction>();
    in_function->name = "in";
    in_function->arguments = expression_list;
    in_function->children.push_back(in_function->arguments);
    in_function->setAlias(or_with_expression.alias);

    /// 2. Insert the new IN expression.

    auto & operands = getFunctionOperands(or_with_expression.or_function);
    operands.push_back(in_function);
}

void LogicalExpressionsOptimizer::cleanupOrExpressions()
{
    /// Saves for each optimized OR-chain the iterator on the first element
    /// list of operands to be deleted.
    std::unordered_map<const ASTFunction *, ASTs::iterator> garbage_map;

    /// Initialization.
    garbage_map.reserve(processed_count);
    for (const auto & chain : disjunctive_equality_chains_map)
    {
        if (!chain.second.is_processed)
            continue;

        const auto & or_with_expression = chain.first;
        auto & operands = getFunctionOperands(or_with_expression.or_function);
        garbage_map.emplace(or_with_expression.or_function, operands.end());
    }

    /// Collect garbage.
    for (const auto & chain : disjunctive_equality_chains_map)
    {
        const auto & equalities = chain.second;
        if (!equalities.is_processed)
            continue;

        const auto & or_with_expression = chain.first;
        auto & operands = getFunctionOperands(or_with_expression.or_function);
        const auto & equality_functions = equalities.functions;

        auto it = garbage_map.find(or_with_expression.or_function);
        if (it == garbage_map.end())
            throw Exception("LogicalExpressionsOptimizer: garbage map is corrupted",
                ErrorCodes::LOGICAL_ERROR);

        auto & first_erased = it->second;
        first_erased = std::remove_if(operands.begin(), first_erased, [&](const ASTPtr & operand)
        {
            return std::binary_search(equality_functions.begin(), equality_functions.end(), &*operand);
        });
    }

    /// Delete garbage.
    for (const auto & entry : garbage_map)
    {
        const auto * function = entry.first;
        auto first_erased = entry.second;

        auto & operands = getFunctionOperands(function);
        operands.erase(first_erased, operands.end());
    }
}

void LogicalExpressionsOptimizer::fixBrokenOrExpressions()
{
    for (const auto & chain : disjunctive_equality_chains_map)
    {
        const auto & equalities = chain.second;
        if (!equalities.is_processed)
            continue;

        const auto & or_with_expression = chain.first;
        const auto * or_function = or_with_expression.or_function;
        auto & operands = getFunctionOperands(or_with_expression.or_function);

        if (operands.size() == 1)
        {
            auto it = or_parent_map.find(or_function);
            if (it == or_parent_map.end())
                throw Exception("LogicalExpressionsOptimizer: parent node information is corrupted",
                    ErrorCodes::LOGICAL_ERROR);
            auto & parents = it->second;

            auto it2 = column_to_position.find(or_function);
            if (it2 != column_to_position.end())
            {
                size_t position = it2->second;
                bool inserted = column_to_position.emplace(operands[0].get(), position).second;
                if (!inserted)
                    throw Exception("LogicalExpressionsOptimizer: internal error", ErrorCodes::LOGICAL_ERROR);
                column_to_position.erase(it2);
            }

            for (auto & parent : parents)
            {
                // The order of children matters if or is children of some function, e.g. minus
                std::replace_if(parent->children.begin(), parent->children.end(),
                    [or_function](const ASTPtr & ptr) { return ptr.get() == or_function; },
                    operands[0]);
            }

            /// If the OR node was the root of the WHERE, PREWHERE, or HAVING expression, then update this root.
            /// Due to the fact that we are dealing with a directed acyclic graph, we must check all cases.
            if (select_query->where() && (or_function == &*(select_query->where())))
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, operands[0]->clone());
            if (select_query->prewhere() && (or_function == &*(select_query->prewhere())))
                select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, operands[0]->clone());
            if (select_query->having() && (or_function == &*(select_query->having())))
                select_query->setExpression(ASTSelectQuery::Expression::HAVING, operands[0]->clone());
        }
    }
}

}
