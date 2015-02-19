#include <DB/Interpreters/LogicalExpressionsOptimizer.h>
#include <DB/Interpreters/Settings.h>

#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Core/ErrorCodes.h>

#include <deque>

namespace DB
{

OrWithExpression::OrWithExpression(ASTFunction * or_function_, const std::string & expression_)
	: or_function(or_function_), expression(expression_)
{
}

bool operator<(const OrWithExpression & lhs, const OrWithExpression & rhs)
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

LogicalExpressionsOptimizer::LogicalExpressionsOptimizer(ASTSelectQuery * select_query_, const Settings & settings_)
	: select_query(select_query_), settings(settings_)
{
}

void LogicalExpressionsOptimizer::optimizeDisjunctiveEqualityChains()
{
	if (select_query == nullptr)
		return;

	collectDisjunctiveEqualityChains();

	for (auto & chain : disjunctive_equality_chains_map)
	{
		if (!mayOptimizeDisjunctiveEqualityChain(chain))
			continue;
		addInExpression(chain);

		auto & equalities = chain.second;
		equalities.is_processed = true;
	}

	cleanupOrExpressions();
	fixBrokenOrExpressions();
}

void LogicalExpressionsOptimizer::collectDisjunctiveEqualityChains()
{
	using Edge = std::pair<IAST *, IAST *>;
	std::deque<Edge> to_visit;

	to_visit.push_back(Edge(nullptr, select_query));
	while (!to_visit.empty())
	{
		auto edge = to_visit.back();
		auto from_node = edge.first;
		auto to_node = edge.second;

		to_visit.pop_back();
		to_node->is_visited = true;

		bool found_chain = false;

		auto function = typeid_cast<ASTFunction *>(to_node);
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
							auto literal = typeid_cast<ASTLiteral *>(&*(equals_expression_list->children[1]));
							if (literal != nullptr)
							{
								auto expr_lhs = equals_expression_list->children[0]->getTreeID();
								OrWithExpression or_with_expression(function, expr_lhs);
								disjunctive_equality_chains_map[or_with_expression].functions.push_back(equals);
								found_chain = true;
							}
						}
					}
				}
			}
		}

		if (found_chain)
		{
			if (from_node != nullptr)
			{
				auto res = or_parent_map.insert(std::make_pair(function, ParentNodes{from_node}));
				if (!res.second)
					throw Exception("Parent node information is corrupted", ErrorCodes::LOGICAL_ERROR);
			}
		}
		else
		{
			for (auto & child : to_node->children)
			{
				if (typeid_cast<ASTSelectQuery *>(&*child) == nullptr)
				{
					if (!child->is_visited)
						to_visit.push_back(Edge(to_node, &*child));
					else
					{
						/// Если узел является функцией OR, обновляем информацию про его родителей.
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

	select_query->clearVisited();

	for (auto & chain : disjunctive_equality_chains_map)
	{
		auto & equalities = chain.second;
		auto & equality_functions = equalities.functions;
		std::sort(equality_functions.begin(), equality_functions.end());
	}
}

bool LogicalExpressionsOptimizer::mayOptimizeDisjunctiveEqualityChain(const DisjunctiveEqualityChain & chain) const
{
	const auto & equalities =  chain.second;
	const auto & equality_functions = equalities.functions;

	/// Исключаем слишком короткие цепочки.
	if (equality_functions.size() < settings.min_or_chain_length_for_optimization)
		return false;

	/// Проверяем, что правые части всех равенств имеют один и тот же тип.
	auto first_expr = static_cast<ASTExpressionList *>(&*(equality_functions[0]->children[0]));
	auto first_literal = static_cast<ASTLiteral *>(&*(first_expr->children[1]));
	for (size_t i = 1; i < equality_functions.size(); ++i)
	{
		auto expr = static_cast<ASTExpressionList *>(&*(equality_functions[i]->children[0]));
		auto literal = static_cast<ASTLiteral *>(&*(expr->children[1]));

		if (literal->type != first_literal->type)
			return false;
	}
	return true;
}

namespace
{

ASTs & getOrFunctionOperands(ASTFunction * or_function)
{
	auto expression_list = static_cast<ASTExpressionList *>(&*(or_function->children[0]));
	return expression_list->children;
}

}

void LogicalExpressionsOptimizer::addInExpression(const DisjunctiveEqualityChain & chain)
{
	using ASTFunctionPtr = Poco::SharedPtr<ASTFunction>;

	const auto & or_with_expression = chain.first;
	const auto & equalities = chain.second;
	const auto & equality_functions = equalities.functions;

	/// 1. Создать новое выражение IN на основе информации из OR-цепочки.

	/// Построить список литералов x1, ..., xN из цепочки expr = x1 OR ... OR expr = xN
	ASTPtr value_list = new ASTExpressionList;
	for (auto function : equality_functions)
	{
		auto expression_list = static_cast<ASTExpressionList *>(&*(function->children[0]));
		ASTPtr literal = expression_list->children[1];
		value_list->children.push_back(literal);
	}

	/// Получить выражение expr из цепочки expr = x1 OR ... OR expr = xN
	ASTPtr equals_expr_lhs;
	{
		auto function = equality_functions[0];
		auto expression_list = static_cast<ASTExpressionList *>(&*(function->children[0]));
		equals_expr_lhs = expression_list->children[0];
	}

	ASTFunctionPtr tuple_function = new ASTFunction;
	tuple_function->name = "tuple";
	tuple_function->arguments = value_list;
	tuple_function->children.push_back(tuple_function->arguments);

	ASTPtr expression_list = new ASTExpressionList;
	expression_list->children.push_back(equals_expr_lhs);
	expression_list->children.push_back(tuple_function);

	/// Построить выражение expr IN (x1, ..., xN)
	ASTFunctionPtr in_function = new ASTFunction;
	in_function->name = "in";
	in_function->arguments = expression_list;
	in_function->children.push_back(in_function->arguments);

	/// 2. Вставить новое выражение IN.

	auto & operands = getOrFunctionOperands(or_with_expression.or_function);
	operands.push_back(in_function);
}

void LogicalExpressionsOptimizer::cleanupOrExpressions()
{
	std::unordered_map<ASTFunction *, ASTs::iterator> garbage_map;
	garbage_map.reserve(or_parent_map.size());

	/// Инициализация.
	for (const auto & chain : disjunctive_equality_chains_map)
	{
		const auto & equalities = chain.second;
		if (!chain.second.is_processed)
			continue;

		const auto & or_with_expression = chain.first;
		auto & operands = getOrFunctionOperands(or_with_expression.or_function);
		garbage_map.insert(std::make_pair(or_with_expression.or_function, operands.end()));
	}

	/// Собрать мусор.
	for (const auto & chain : disjunctive_equality_chains_map)
	{
		const auto & equalities = chain.second;
		if (!equalities.is_processed)
			continue;

		const auto & or_with_expression = chain.first;
		auto & operands = getOrFunctionOperands(or_with_expression.or_function);
		const auto & equality_functions = equalities.functions;

		auto it = garbage_map.find(or_with_expression.or_function);
		if (it == garbage_map.end())
			throw Exception("Garbage map is corrupted", ErrorCodes::LOGICAL_ERROR);

		auto & last = it->second;
		last = std::remove_if(operands.begin(), last, [&](const ASTPtr & operand)
		{
			return std::binary_search(equality_functions.begin(), equality_functions.end(), &*operand);
		});
	}

	/// Удалить мусор.
	for (const auto & entry : garbage_map)
	{
		auto & operands = getOrFunctionOperands(entry.first);
		operands.erase(entry.second, operands.end());
	}
}

void LogicalExpressionsOptimizer::fixBrokenOrExpressions()
{
	for (const auto & chain : disjunctive_equality_chains_map)
	{
		const auto & or_with_expression = chain.first;
		auto or_function = or_with_expression.or_function;
		auto & operands = getOrFunctionOperands(or_with_expression.or_function);

		if (operands.size() == 1)
		{
			auto it = or_parent_map.find(or_function);
			if (it == or_parent_map.end())
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
			if (!select_query->where_expression.isNull() && (or_function == &*(select_query->where_expression)))
				select_query->where_expression = operands[0];
			if (!select_query->prewhere_expression.isNull() && (or_function == &*(select_query->prewhere_expression)))
				select_query->prewhere_expression = operands[0];
			if (!select_query->having_expression.isNull() && (or_function == &*(select_query->having_expression)))
				select_query->having_expression = operands[0];
		}
	}
}

}
