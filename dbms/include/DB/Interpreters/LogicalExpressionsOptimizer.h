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

/** Этот класс предоставляет функции для оптимизации логических выражений внутри запросов.
  *
  * Для простоты назовём однородной OR-цепочой любое выражение имеющее следующую структуру:
  * expr = x1 OR ... OR expr = xN
  * где expr - произвольное выражение и x1, ..., xN - литералы одного типа
  */
class LogicalExpressionsOptimizer
{
public:
	LogicalExpressionsOptimizer(ASTPtr root_);

	/** Заменить все довольно длинные однородные OR-цепочки expr = x1 OR ... OR expr = xN
	  * на выражения expr IN (x1, ..., xN).
	  */
	void optimizeDisjunctiveEqualityChains();

	LogicalExpressionsOptimizer(const LogicalExpressionsOptimizer &) = delete;
	LogicalExpressionsOptimizer & operator=(const LogicalExpressionsOptimizer &) = delete;

private:
	using Equalities = std::vector<ASTFunction *>;
	using DisjunctiveEqualitiesMap = std::map<LogicalOrWithLeftHandSide, Equalities>;
	using DisjunctiveEqualityChain = DisjunctiveEqualitiesMap::value_type;

	using ASTFunctionPtr = Poco::SharedPtr<ASTFunction>;
	using IASTs = std::vector<IAST *>;
	using ParentMap = std::map<ASTFunction *, IASTs>;

private:
	/// Собрать информацию про все равенства входящие в цепочки OR (не обязательно однородные).
	void collectDisjunctiveEqualityChains();

	/** Проверить, что множество равенств expr = x1, ..., expr = xN выполняет два следующих требования:
	  * 1. Оно не слишком маленькое
	  * 2. x1, ... xN имеют один и тот же тип
	  */
	bool mustTransform(const Equalities & equalities) const;

	/// Создать новое выражение IN на основе цепочки OR.
	ASTFunctionPtr createInExpression(const Equalities & equalities) const;

	/// Вставить новое выражение IN в запрос.
	void putInExpression(const DisjunctiveEqualityChain & chain, ASTFunctionPtr in_expression);

    /// Удалить узлы OR, которые имеют только один узел типа Function.
	void fixBrokenOrExpressions();

private:
	ASTPtr root;
	ASTSelectQuery * select_query;
	/// Информация про OR-цепочки внутри запроса.
	DisjunctiveEqualitiesMap disjunctive_equalities_map;
	// Родители функций OR.
	ParentMap parent_map;
};

}