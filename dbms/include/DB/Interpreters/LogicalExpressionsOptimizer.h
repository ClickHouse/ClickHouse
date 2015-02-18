#pragma once

#include <DB/Parsers/IAST.h>

#include <string>
#include <vector>
#include <map>
#include <unordered_map>

namespace DB
{

class ASTFunction;
class ASTSelectQuery;

/** Функция OR с выражением.
  */
struct OrWithExpression
{
	OrWithExpression(ASTFunction * or_function_, const std::string & expression_);
	ASTFunction * or_function;
	const std::string expression;
};

/** Этот класс предоставляет функции для оптимизации логических выражений внутри запросов.
  *
  * Для простоты назовём однородной OR-цепочой любое выражение имеющее следующую структуру:
  * expr = x1 OR ... OR expr = xN
  * где expr - произвольное выражение и x1, ..., xN - литералы одного типа
  */
class LogicalExpressionsOptimizer final
{
public:
	/// Конструктор. Принимает корень DAG запроса.
	LogicalExpressionsOptimizer(ASTSelectQuery * select_query_);

	/** Заменить все довольно длинные однородные OR-цепочки expr = x1 OR ... OR expr = xN
	  * на выражения expr IN (x1, ..., xN).
	  */
	void optimizeDisjunctiveEqualityChains();

	LogicalExpressionsOptimizer(const LogicalExpressionsOptimizer &) = delete;
	LogicalExpressionsOptimizer & operator=(const LogicalExpressionsOptimizer &) = delete;

private:
	using Equalities = std::vector<ASTFunction *>;
	using DisjunctiveEqualitiesMap = std::map<OrWithExpression, Equalities>;
	using DisjunctiveEqualityChain = DisjunctiveEqualitiesMap::value_type;

	using ParentNodes = std::vector<IAST *>;
	using FunctionParentMap = std::unordered_map<ASTFunction *, ParentNodes>;

private:
	/** Собрать информация про все равенства входящие в цепочки OR (не обязательно однородные).
	  * Эта информация сгруппирована по выражению, которое стоит в левой части равенства.
	  */
	void collectDisjunctiveEqualityChains();

	/** Проверить, что множество равенств expr = x1, ..., expr = xN выполняет два следующих требования:
	  * 1. Оно не слишком маленькое
	  * 2. x1, ... xN имеют один и тот же тип
	  */
	bool mayOptimizeDisjunctiveEqualityChain(const DisjunctiveEqualityChain & chain) const;

	/// Заменить однородную OR-цепочку на выражение IN.
	void replaceOrByIn(const DisjunctiveEqualityChain & chain);

    /// Удалить выражения OR, которые имеют только один операнд.
	void fixBrokenOrExpressions();

private:
	ASTSelectQuery * select_query;
	/// Информация про OR-цепочки внутри запроса.
	DisjunctiveEqualitiesMap disjunctive_equalities_map;
	/// Родители функций OR.
	FunctionParentMap or_parent_map;
};

}