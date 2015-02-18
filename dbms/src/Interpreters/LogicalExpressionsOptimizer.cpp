#include <DB/Interpreters/LogicalExpressionsOptimizer.h>

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/queryToString.h>

#include <DB/Core/ErrorCodes.h>

#include <deque>

namespace DB
{

LogicalOrWithLeftHandSide::LogicalOrWithLeftHandSide(ASTFunction * or_function_, const std::string & expression_)
	: or_function(or_function_), expression(expression_)
{
}

bool operator<(const LogicalOrWithLeftHandSide & lhs, const LogicalOrWithLeftHandSide & rhs)
{
	std::ptrdiff_t res1 = lhs.or_function - rhs.or_function;
	if (res1 < 0)
		return true;
	if (res1 > 0)
		return false;

	int res2 = lhs.expression.compare(rhs.expression);
	if (res2 < 0)
		return true;
	if (res2 > 0)
		return false;

	return false;
}

LogicalExpressionsOptimizer::LogicalExpressionsOptimizer(ASTPtr root_)
	: root(root_), select_query(typeid_cast<ASTSelectQuery *>(&*root))
{
}

void LogicalExpressionsOptimizer::optimizeDisjunctiveEqualityChains()
{
	if (select_query == nullptr)
		return;

	collectDisjunctiveEqualityChains();

	for (const auto & chain : disjunctive_equalities_map)
	{
		if (!mayOptimizeDisjunctiveEqualityChain(chain))
			continue;

		const auto & equalities = chain.second;
		auto in_expression = createInExpression(equalities);
		putInExpression(chain, in_expression);
	}

	fixBrokenOrExpressions();
}

void LogicalExpressionsOptimizer::collectDisjunctiveEqualityChains()
{
	using Edge = std::pair<ASTPtr, ASTPtr>;
	std::deque<Edge> to_visit;

	to_visit.push_back(Edge(ASTPtr(), root));
	while (!to_visit.empty())
	{
		auto edge = to_visit.front();
		auto & from_node = edge.first;
		auto & to_node = edge.second;

		to_visit.pop_front();

		bool found_chain = false;

		auto function = typeid_cast<ASTFunction *>(&*to_node);
		if ((function != nullptr) && (function->name == "or") && (function->children.size() == 1))
		{
			auto expression_list = typeid_cast<ASTExpressionList *>(&*(function->children[0]));
			if (expression_list != nullptr)
			{
				/// Цепочка элементов выражения OR.
				for (auto child : expression_list->children)
				{
					auto equals = typeid_cast<ASTFunction *>(&*child);
					if ((equals != nullptr) && (equals->name == "equals") && (equals->children.size() == 1))
					{
						auto equals_expression_list = typeid_cast<ASTExpressionList *>(&*(equals->children[0]));
						if ((equals_expression_list != nullptr) && (equals_expression_list->children.size() == 2))
						{
							/// Равенство expr = xN.
							auto expr_lhs = queryToString(equals_expression_list->children[0]);

							auto literal = typeid_cast<ASTLiteral *>(&*(equals_expression_list->children[1]));
							if (literal != nullptr)
							{
								LogicalOrWithLeftHandSide logical_or_with_lhs(function, expr_lhs);
								disjunctive_equalities_map[logical_or_with_lhs].push_back(equals);
								found_chain = true;
							}
						}
					}
				}
			}

			if (!from_node.isNull() && found_chain)
				parent_map[function].push_back(from_node.get());
		}

		if (!found_chain)
			for (auto & child : to_node->children)
				if (typeid_cast<ASTSelectQuery *>(&*child) == nullptr)
					to_visit.push_back(Edge(to_node, child));
	}

	for (auto & chain : disjunctive_equalities_map)
	{
		auto & equalities = chain.second;
		std::sort(equalities.begin(), equalities.end());
	}
}

bool LogicalExpressionsOptimizer::mayOptimizeDisjunctiveEqualityChain(const DisjunctiveEqualityChain & chain) const
{
	const UInt64 mutation_threshold = 3;
	const auto & equalities = chain.second;

	/// Исключаем слишком короткие цепочки.
	if (equalities.size() < mutation_threshold)
		return false;

	/// Проверяем, что правые части всех равенств имеют один и тот же тип.
	auto first_expr = static_cast<ASTExpressionList *>(&*(equalities[0]->children[0]));
	auto first_literal = static_cast<ASTLiteral *>(&*(first_expr->children[1]));
	for (size_t i = 1; i < equalities.size(); ++i)
	{
		auto expr = static_cast<ASTExpressionList *>(&*(equalities[i]->children[0]));
		auto literal = static_cast<ASTLiteral *>(&*(expr->children[1]));

		if (literal->type != first_literal->type)
			return false;
	}
	return true;
}

LogicalExpressionsOptimizer::ASTFunctionPtr LogicalExpressionsOptimizer::createInExpression(const Equalities & equalities) const
{
	ASTPtr value_list = new ASTExpressionList;
	for (auto function : equalities)
	{
		auto expr = static_cast<ASTExpressionList *>(&*(function->children[0]));
		auto literal = static_cast<ASTLiteral *>(&*(expr->children[1]));
		value_list->children.push_back(literal->clone());
	}

	auto function = equalities[0];
	auto expr = static_cast<ASTExpressionList *>(&*(function->children[0]));
	auto equals_expr_lhs = static_cast<ASTLiteral *>(&*(expr->children[0]));

	ASTFunctionPtr tuple_function = new ASTFunction;
	tuple_function->name = "tuple";
	tuple_function->arguments = value_list;
	tuple_function->children.push_back(tuple_function->arguments);

	ASTPtr in_expr = new ASTExpressionList;
	in_expr->children.push_back(equals_expr_lhs->clone());
	in_expr->children.push_back(tuple_function);

	ASTFunctionPtr in_function = new ASTFunction;
	in_function->name = "in";
	in_function->arguments = in_expr;
	in_function->children.push_back(in_function->arguments);

	return in_function;
}

namespace
{

ASTs & getOrExpressionOperands(const LogicalOrWithLeftHandSide & logical_or_with_lhs)
{
	auto or_function = logical_or_with_lhs.or_function;
	auto expression_list = static_cast<ASTExpressionList *>(&*(or_function->children[0]));
	return expression_list->children;
}

}

void LogicalExpressionsOptimizer::putInExpression(const DisjunctiveEqualityChain & chain, ASTFunctionPtr in_expression)
{
	const auto & logical_or_with_lhs = chain.first;
	const auto & equalities = chain.second;

	auto & operands = getOrExpressionOperands(logical_or_with_lhs);
	operands.push_back(in_expression);

	auto it = std::remove_if(operands.begin(), operands.end(), [&](const ASTPtr & node)
	{
		return std::binary_search(equalities.begin(), equalities.end(), node.get());
	});
	operands.erase(it, operands.end());
}

void LogicalExpressionsOptimizer::fixBrokenOrExpressions()
{
	for (const auto & chain : disjunctive_equalities_map)
	{
		const auto & logical_or_with_lhs = chain.first;
		auto or_function = logical_or_with_lhs.or_function;
		auto & operands = getOrExpressionOperands(logical_or_with_lhs);

		if (operands.size() == 1)
		{
			auto it = parent_map.find(or_function);
			if (it == parent_map.end())
				throw Exception("Parent node information is corrupted", ErrorCodes::LOGICAL_ERROR);
			auto & parents = it->second;

			for (auto & parent : parents)
			{
				parent->children.push_back(operands[0]);
				auto it = std::remove(parent->children.begin(), parent->children.end(), or_function);
				parent->children.erase(it, parent->children.end());
			}

			/// Если узел OR был корнем выражения WHERE, PREWHERE или HAVING, то следует обновить этот корень.
			/// Из-за того, что имеем дело с направленным ациклическим графом, надо проверить все случаи.
			if (or_function == select_query->where_expression.get())
				select_query->where_expression = operands[0];
			if (or_function == select_query->prewhere_expression.get())
				select_query->prewhere_expression = operands[0];
			if (or_function == select_query->having_expression.get())
				select_query->having_expression = operands[0];
		}
	}
}

}
