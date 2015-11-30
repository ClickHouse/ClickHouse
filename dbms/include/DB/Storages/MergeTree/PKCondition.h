#pragma once

#include <sstream>

#include <DB/Core/FieldVisitors.h>
#include <DB/Interpreters/Context.h>
#include <DB/Core/SortDescription.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Storages/MergeTree/BoolMask.h>
#include <DB/Functions/IFunction.h>


namespace DB
{


/** Диапазон с открытыми или закрытыми концами; возможно, неограниченный.
  */
struct Range
{
private:
	static bool equals(const Field & lhs, const Field & rhs);
	static bool less(const Field & lhs, const Field & rhs);

public:
	Field left;				/// левая граница, если есть
	Field right;			/// правая граница, если есть
	bool left_bounded = false;		/// ограничен ли слева
	bool right_bounded = false; 	/// ограничен ли справа
	bool left_included = false; 	/// включает левую границу, если есть
	bool right_included = false;	/// включает правую границу, если есть

	/// Всё множество.
	Range() {}

	/// Одна точка.
	Range(const Field & point) : left(point), right(point), left_bounded(true), right_bounded(true), left_included(true), right_included(true) {}

	/// Ограниченный с двух сторон диапазон.
	Range(const Field & left_, bool left_included_, const Field & right_, bool right_included_)
		: left(left_), right(right_), left_bounded(true), right_bounded(true), left_included(left_included_), right_included(right_included_) {}

	static Range createRightBounded(const Field & right_point, bool right_included)
	{
		Range r;
		r.right = right_point;
		r.right_bounded = true;
		r.right_included = right_included;
		return r;
	}

	static Range createLeftBounded(const Field & left_point, bool left_included)
	{
		Range r;
		r.left = left_point;
		r.left_bounded = true;
		r.left_included = left_included;
		return r;
	}

	/// Установить левую границу.
	void setLeft(const Field & point, bool included)
	{
		left = point;
		left_bounded = true;
		left_included = included;
	}

	/// Установить правую границу.
	void setRight(const Field & point, bool included)
	{
		right = point;
		right_bounded = true;
		right_included = included;
	}

	/// x входит в range
	bool contains(const Field & x) const
	{
		return !leftThan(x) && !rightThan(x);
	}

	/// x находится левее
	bool rightThan(const Field & x) const
	{
		return (left_bounded
			? !(less(left, x) || (left_included && equals(x, left)))
			: false);
	}

	/// x находится правее
	bool leftThan(const Field & x) const
	{
		return (right_bounded
			? !(less(x, right) || (right_included && equals(x, right)))
			: false);
	}

	bool intersectsRange(const Range & r) const
	{
		/// r левее меня.
		if (r.right_bounded
			&& left_bounded
			&& (less(r.right, left)
				|| ((!left_included || !r.right_included)
					&& equals(r.right, left))))
			return false;

		/// r правее меня.
		if (r.left_bounded
			&& right_bounded
			&& (less(right, r.left)							/// ...} {...
				|| ((!right_included || !r.left_included)	/// ...)[...  или ...](...
					&& equals(r.left, right))))
			return false;

		return true;
	}

	bool containsRange(const Range & r) const
	{
		/// r начинается левее меня.
		if (left_bounded
			&& (!r.left_bounded
				|| less(r.left, left)
				|| (r.left_included
					&& !left_included
					&& equals(r.left, left))))
			return false;

		/// r заканчивается правее меня.
		if (right_bounded
			&& (!r.right_bounded
				|| less(right, r.right)
				|| (r.right_included
					&& !right_included
					&& equals(r.right, right))))
			return false;

		return true;
	}

	void swapLeftAndRight()
	{
		std::swap(left, right);
		std::swap(left_bounded, right_bounded);
		std::swap(left_included, right_included);
	}

	String toString() const
	{
		std::stringstream str;

		if (!left_bounded)
			str << "(-inf, ";
		else
			str << (left_included ? '[' : '(') << apply_visitor(FieldVisitorToString(), left) << ", ";

		if (!right_bounded)
			str << "+inf)";
		else
			str << apply_visitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

		return str.str();
	}
};

class ASTSet;


/** Условие на индекс.
  *
  * Состоит из условий на принадлежность ключа всевозможным диапазонам или множествам,
  *  а также логических связок AND/OR/NOT над этими условиями.
  *
  * Составляет reverse polish notation от этих условий
  *  и умеет вычислять (интерпретировать) её выполнимость над диапазонами ключа.
  */
class PKCondition
{
	struct RPNElement;

public:
	/// Словарь, содержащий действия к соответствующим функциям по превращению их в RPNElement
	using AtomMap = std::unordered_map<std::string, void(*)(RPNElement & out, const Field & value, ASTPtr & node)>;
	static const AtomMap atom_map;

	/// Не учитывает секцию SAMPLE. all_columns - набор всех столбцов таблицы.
	PKCondition(ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns, const SortDescription & sort_descr);

	/// Выполнимо ли условие в диапазоне ключей.
	/// left_pk и right_pk должны содержать все поля из sort_descr в соответствующем порядке.
	/// data_types - типы столбцов первичного ключа.
	bool mayBeTrueInRange(const Field * left_pk, const Field * right_pk, const DataTypes & data_types) const;

	/// Выполнимо ли условие в полубесконечном (не ограниченном справа) диапазоне ключей.
	/// left_pk должен содержать все поля из sort_descr в соответствующем порядке.
	bool mayBeTrueAfter(const Field * left_pk, const DataTypes & data_types) const;

	/// Проверяет, что индекс не может быть использован.
	bool alwaysUnknown() const;

	/// Наложить дополнительное условие: значение в столбце column должно быть в диапазоне range.
	/// Возвращает, есть ли такой столбец в первичном ключе.
	bool addCondition(const String & column, const Range & range);

	String toString() const;

	/** Вычисление выражений, зависящих только от констант.
	 * Чтобы индекс мог использоваться, если написано, например WHERE Date = toDate(now()).
	 */
	static Block getBlockWithConstants(
		const ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns);
private:
	/// Выражение хранится в виде обратной польской строки (Reverse Polish Notation).
	struct RPNElement
	{
		enum Function
		{
			/// Атомы логического выражения.
			FUNCTION_IN_RANGE,
			FUNCTION_NOT_IN_RANGE,
			FUNCTION_IN_SET,
			FUNCTION_NOT_IN_SET,
			FUNCTION_UNKNOWN, /// Может принимать любое значение.
			/// Операторы логического выражения.
			FUNCTION_NOT,
			FUNCTION_AND,
			FUNCTION_OR,
		};

		RPNElement() {}
		RPNElement(Function function_) : function(function_) {}
		RPNElement(Function function_, size_t key_column_) : function(function_), key_column(key_column_) {}
		RPNElement(Function function_, size_t key_column_, const Range & range_)
			: function(function_), range(range_), key_column(key_column_) {}

		String toString() const;

		Function function = FUNCTION_UNKNOWN;

		/// Для FUNCTION_IN_RANGE и FUNCTION_NOT_IN_RANGE.
		Range range;
		size_t key_column;
		/// Для FUNCTION_IN_SET, FUNCTION_NOT_IN_SET
		ASTPtr in_function;

		/** Цепочка возможно-монотонных функций.
		  * Если столбец первичного ключа завёрнут в функции, которые могут быть монотонными в некоторых диапазонах значений
		  * (например: -toFloat64(toDayOfWeek(date))), то здесь будут расположены функции: toDayOfWeek, toFloat64, negate.
		  */
		using MonotonicFunctionsChain = std::vector<FunctionPtr>;
		mutable MonotonicFunctionsChain monotonic_functions_chain;	/// Выполнение функции не нарушает константность.
	};

	typedef std::vector<RPNElement> RPN;
	typedef std::map<String, size_t> ColumnIndices;

	bool mayBeTrueInRange(const Field * left_pk, const Field * right_pk, const DataTypes & data_types, bool right_bounded) const;

	void traverseAST(ASTPtr & node, const Context & context, Block & block_with_constants);
	bool atomFromAST(ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out);
	bool operatorFromAST(const ASTFunction * func, RPNElement & out);

	/** Является ли node столбцом первичного ключа
	  *  или выражением, в котором столбец первичного ключа завёрнут в цепочку функций,
	  *  которые могут быть монотонными на некоторых диапазонах.
	  * Если да - вернуть номер этого столбца в первичном ключе, а также заполнить цепочку возможно-монотонных функций.
	  */
	bool isPrimaryKeyPossiblyWrappedByMonotonicFunctions(
		const ASTPtr & node,
		const Context & context,
		size_t & out_primary_key_column_num,
		RPNElement::MonotonicFunctionsChain & out_functions_chain);

	bool isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(
		const ASTPtr & node,
		size_t & out_primary_key_column_num,
		std::vector<const ASTFunction *> & out_functions_chain);

	RPN rpn;

	SortDescription sort_descr;
	ColumnIndices pk_columns;
};

}
