#pragma once

#include <DB/Parsers/IAST.h>

#include <string>
#include <vector>
#include <map>
#include <unordered_map>

namespace DB
{

class Settings;
class ASTFunction;
class ASTSelectQuery;

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
	LogicalExpressionsOptimizer(ASTSelectQuery * select_query_, const Settings & settings_);

	/** Заменить все довольно длинные однородные OR-цепочки expr = x1 OR ... OR expr = xN
	  * на выражения expr IN (x1, ..., xN).
	  */
	void optimizeDisjunctiveEqualityChains();

	LogicalExpressionsOptimizer(const LogicalExpressionsOptimizer &) = delete;
	LogicalExpressionsOptimizer & operator=(const LogicalExpressionsOptimizer &) = delete;

private:
	/** Функция OR с выражением.
	*/
	struct OrWithExpression
	{
		OrWithExpression(ASTFunction * or_function_, const std::string & expression_);
		bool operator<(const OrWithExpression & rhs) const;

		ASTFunction * or_function;
		const std::string expression;
	};

	struct Equalities
	{
		std::vector<ASTFunction *> functions;
		bool is_processed = false;
	};

	using DisjunctiveEqualityChainsMap = std::map<OrWithExpression, Equalities>;
	using DisjunctiveEqualityChain = DisjunctiveEqualityChainsMap::value_type;

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

	/// Вставить выражение IN в OR-цепочку.
	void addInExpression(const DisjunctiveEqualityChain & chain);

	/// Удалить равенства, которые были заменены выражениями IN.
	void cleanupOrExpressions();

	/// Удалить выражения OR, которые имеют только один операнд.
	void fixBrokenOrExpressions();

private:
	using ParentNodes = std::vector<IAST *>;
	using FunctionParentMap = std::unordered_map<IAST *, ParentNodes>;

private:
	ASTSelectQuery * select_query;
	const Settings & settings;
	bool hasOptimizedDisjunctiveEqualityChains = false;
	/// Информация про OR-цепочки внутри запроса.
	DisjunctiveEqualityChainsMap disjunctive_equality_chains_map;
	/// Количество обработанных OR-цепочек.
	size_t processed_count = 0;
	/// Родители функций OR.
	FunctionParentMap or_parent_map;
};

}
