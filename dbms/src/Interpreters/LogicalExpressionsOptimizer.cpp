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
	if (lhs.or_function < rhs.or_function)
		return true;
	if (lhs.or_function > rhs.or_function)
		return false;

	int val = lhs.expression.compare(rhs.expression);
	if (val < 0)
		return true;
	if (val > 0)
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

	/** 1. Поиск всех цепочек OR.
	  */
	collectDisjunctiveEqualityChains();

	/** 2. Заменяем длинные цепочки на выражения IN.
	  */
	for (const auto & e : disjunctive_equalities_map)
	{
		const LogicalOrWithLeftHandSide & logical_or_with_lhs = e.first; 
		const Equalities & equalities = e.second;

		/** Пропустить цепочку, если она слишком коротка или содержит данные разних типов.
		  */

		if (!mustTransform(equalities))
			continue;

		/** Создать новое выражение IN.
		  */

		auto in_expr = createInExpression(logical_or_with_lhs, equalities);

		/** Вставить это выражение в запрос.
		  */

		ASTFunction * or_function = logical_or_with_lhs.or_function;
		ASTExpressionList * expression_list = static_cast<ASTExpressionList *>(&*(or_function->children[0]));
		auto & children = expression_list->children;

		children.push_back(in_expr);

		auto it = std::remove_if(children.begin(), children.end(), [&](const ASTPtr & node)
		{
			return std::binary_search(equalities.begin(), equalities.end(), node.get());
		});
		children.erase(it, children.end());
	}

	/** 3. Удалить узлы OR, которые имеют только один узел типа Function.
	  */
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

		ASTFunction * function = typeid_cast<ASTFunction *>(&*to_node);
		if ((function != nullptr) && (function->name == "or") && (function->children.size() == 1))
		{
			if (!from_node.isNull())
				parent_map[function].push_back(from_node.get());

			ASTExpressionList * expression_list = typeid_cast<ASTExpressionList *>(&*(function->children[0]));
			if (expression_list != nullptr)
			{
				/// Цепочка элементов выражения OR.
				for (auto child : expression_list->children)
				{
					ASTFunction * equals = typeid_cast<ASTFunction *>(&*child);
					if ((equals != nullptr) && (equals->name == "equals") && (equals->children.size() == 1))
					{
						ASTExpressionList * equals_expression_list = typeid_cast<ASTExpressionList *>(&*(equals->children[0]));
						if ((equals_expression_list != nullptr) && (equals_expression_list->children.size() == 2))
						{
							// Равенство c = xk
							auto expr_lhs = queryToString(equals_expression_list->children[0]);

							ASTLiteral * literal = typeid_cast<ASTLiteral *>(&*(equals_expression_list->children[1]));
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

	for (auto & e : disjunctive_equalities_map)
	{
		Equalities & equalities = e.second;
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

/// Создать новое выражение IN на основе цепочки OR.
/// Предполагается, что все нужны проверки уже сделаны в optimizeDisjunctiveEqualityChains.
LogicalExpressionsOptimizer::ASTFunctionPtr LogicalExpressionsOptimizer::createInExpression(const LogicalOrWithLeftHandSide & logical_or_with_lhs, const Equalities & equalities)
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

void LogicalExpressionsOptimizer::fixBrokenOrExpressions()
{
	for (const auto & e : disjunctive_equalities_map)
	{
		const LogicalOrWithLeftHandSide & logical_or_with_lhs = e.first;
		const Equalities & equalities = e.second;

		ASTFunction * or_function = logical_or_with_lhs.or_function;
		ASTExpressionList * expression_list = static_cast<ASTExpressionList *>(&*(or_function->children[0]));
		auto & children = expression_list->children;

		if (children.size() == 1)
		{
			std::vector<IAST *> & parents = parent_map[or_function];
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
