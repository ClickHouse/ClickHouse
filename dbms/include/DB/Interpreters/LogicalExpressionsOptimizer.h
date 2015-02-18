#pragma once

#include <DB/Parsers/ASTFunction.h>

namespace DB
{

class IAST;
class ASTSelectQuery;

struct LogicalOrWithLeftHandSide
{
	LogicalOrWithLeftHandSide(ASTFunction * or_function_, const std::string & expression_);
	ASTFunction * or_function;
	const std::string expression;
};

class LogicalExpressionsOptimizer
{
public:
	LogicalExpressionsOptimizer(ASTPtr root_);
	void optimizeDisjunctiveEqualityChains();

	LogicalExpressionsOptimizer(const LogicalExpressionsOptimizer &) = delete;
	LogicalExpressionsOptimizer & operator=(const LogicalExpressionsOptimizer &) = delete;

private:
	using Equalities = std::vector<ASTFunction *>;

	/// Цепочки x = c[1] OR x = c[2] OR ... x = C[N], где x - произвольное выражение
	/// и c[1], c[2], ..., c[N] - литералы одного типа.
	using DisjunctiveEqualitiesMap = std::map<LogicalOrWithLeftHandSide, Equalities>;

	using ASTFunctionPtr = Poco::SharedPtr<ASTFunction>;

	using ParentMap = std::map<ASTFunction *, std::vector<IAST *> >;

private:
	void collectDisjunctiveEqualityChains();
	bool mustTransform(const Equalities & equalities) const;
	ASTFunctionPtr createInExpression(const LogicalOrWithLeftHandSide & logical_or_with_lhs, const Equalities & equalities);
	void fixBrokenOrExpressions();

private:
	ASTPtr root;
	ASTSelectQuery * select_query;
	DisjunctiveEqualitiesMap disjunctive_equalities_map;
	ParentMap parent_map;
};

}