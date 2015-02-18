#include <DB/Interpreters/LogicalExpressionsOptimizer.h>

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/queryToString.h>

#include <deque>

namespace DB
{

LogicalOrWithLeftHandSide::LogicalOrWithLeftHandSide(ASTFunction * or_function_, const std::string & expression_)
	: or_function(or_function_), expression(expression_)
{
}

bool operator<(const LogicalOrWithLeftHandSide & lhs, 
			   const LogicalOrWithLeftHandSide & rhs)
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
		const auto & equalities = chain.second;

		if (!mustTransform(equalities))
			continue;

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

		bool found = false;

		auto function = typeid_cast<ASTFunction *>(&*to_node);
		if ((function != nullptr) && (function->name == "or") && (function->children.size() == 1))
		{
			if (!from_node.isNull())
				parent_map[function].push_back(from_node.get());

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
							// Равенство c = xk
							auto expr_lhs = queryToString(equals_expression_list->children[0]);

							auto literal = typeid_cast<ASTLiteral *>(&*(equals_expression_list->children[1]));
							if (literal != nullptr)
							{
								LogicalOrWithLeftHandSide logical_or_with_lhs(function, expr_lhs);
								disjunctive_equalities_map[logical_or_with_lhs].push_back(equals);
								found = true;
							}
						}
					}
				}
			}
		}

		if (!found)
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

bool LogicalExpressionsOptimizer::mustTransform(const Equalities & equalities) const
{
	const UInt64 mutation_threshold = 3;

	if (equalities.size() < mutation_threshold)
		return false;

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

void LogicalExpressionsOptimizer::putInExpression(const DisjunctiveEqualityChain & chain, ASTFunctionPtr in_expression)
{
	const auto & logical_or_with_lhs = chain.first;
	const auto & equalities = chain.second;

	auto or_function = logical_or_with_lhs.or_function;
	auto expression_list = static_cast<ASTExpressionList *>(&*(or_function->children[0]));
	auto & children = expression_list->children;

	children.push_back(in_expression);

	auto it = std::remove_if(children.begin(), children.end(), [&](const ASTPtr & node)
	{
		return std::binary_search(equalities.begin(), equalities.end(), node.get());
	});
	children.erase(it, children.end());
}

void LogicalExpressionsOptimizer::fixBrokenOrExpressions()
{
	for (const auto & chain : disjunctive_equalities_map)
	{
		const auto & logical_or_with_lhs = chain.first;
		const auto & equalities = chain.second;

		auto or_function = logical_or_with_lhs.or_function;
		auto expression_list = static_cast<ASTExpressionList *>(&*(or_function->children[0]));
		auto & children = expression_list->children;

		if (children.size() == 1)
		{
			auto & parents = parent_map[or_function];
			for (auto & parent : parents)
			{
				parent->children.push_back(children[0]);
				auto it = std::remove(parent->children.begin(), parent->children.end(), or_function);
				parent->children.erase(it, parent->children.end());
			}

			/// Если узел OR был корнем выражения WHERE, PREWHERE или HAVING, то следует обновить этот корень.
			if (or_function == select_query->where_expression.get())
				select_query->where_expression = children[0];
			else if (or_function == select_query->prewhere_expression.get())
				select_query->prewhere_expression = children[0];
			else if (or_function == select_query->having_expression.get())
				select_query->having_expression = children[0];
		}
	}
}

}
