#pragma once

#include <sstream>

#include <DB/Interpreters/Context.h>
#include <DB/Core/SortDescription.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Storages/MergeTree/BoolMask.h>


namespace DB
{


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

/** Более точное сравнение.
  * Отличается от Field::operator< и Field::operator== тем, что сравнивает значения разных числовых типов между собой.
  * Правила сравнения - такие же, что и в FunctionsComparison.
  * В том числе, сравнение знаковых и беззнаковых оставляем UB.
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    bool operator() (const Null & l, const Null & r)        const { return true; }
    bool operator() (const Null & l, const UInt64 & r)      const { return false; }
    bool operator() (const Null & l, const Int64 & r)       const { return false; }
    bool operator() (const Null & l, const Float64 & r)     const { return false; }
    bool operator() (const Null & l, const String & r)      const { return false; }
    bool operator() (const Null & l, const Array & r)       const { return false; }

    bool operator() (const UInt64 & l, const Null & r)      const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l == r; }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return l == r; }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return l == r; }
    bool operator() (const UInt64 & l, const String & r)    const { return false; }
    bool operator() (const UInt64 & l, const Array & r)     const { return false; }

    bool operator() (const Int64 & l, const Null & r)       const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return l == r; }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l == r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return l == r; }
    bool operator() (const Int64 & l, const String & r)     const { return false; }
    bool operator() (const Int64 & l, const Array & r)      const { return false; }

    bool operator() (const Float64 & l, const Null & r)     const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return l == r; }
    bool operator() (const Float64 & l, const Int64 & r)    const { return l == r; }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l == r; }
    bool operator() (const Float64 & l, const String & r)   const { return false; }
    bool operator() (const Float64 & l, const Array & r)    const { return false; }

    bool operator() (const String & l, const Null & r)      const { return false; }
    bool operator() (const String & l, const UInt64 & r)    const { return false; }
    bool operator() (const String & l, const Int64 & r)     const { return false; }
    bool operator() (const String & l, const Float64 & r)   const { return false; }
    bool operator() (const String & l, const String & r)    const { return l == r; }
    bool operator() (const String & l, const Array & r)     const { return false; }

    bool operator() (const Array & l, const Null & r)       const { return false; }
    bool operator() (const Array & l, const UInt64 & r)     const { return false; }
    bool operator() (const Array & l, const Int64 & r)      const { return false; }
    bool operator() (const Array & l, const Float64 & r)    const { return false; }
    bool operator() (const Array & l, const String & r)     const { return false; }
    bool operator() (const Array & l, const Array & r)      const { return l == r; }
};

class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    bool operator() (const Null & l, const Null & r)        const { return false; }
    bool operator() (const Null & l, const UInt64 & r)      const { return true; }
    bool operator() (const Null & l, const Int64 & r)       const { return true; }
    bool operator() (const Null & l, const Float64 & r)     const { return true; }
    bool operator() (const Null & l, const String & r)      const { return true; }
    bool operator() (const Null & l, const Array & r)       const { return true; }

    bool operator() (const UInt64 & l, const Null & r)      const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l < r; }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return l < r; }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return l < r; }
    bool operator() (const UInt64 & l, const String & r)    const { return true; }
    bool operator() (const UInt64 & l, const Array & r)     const { return true; }

    bool operator() (const Int64 & l, const Null & r)       const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return l < r; }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l < r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return l < r; }
    bool operator() (const Int64 & l, const String & r)     const { return true; }
    bool operator() (const Int64 & l, const Array & r)      const { return true; }

    bool operator() (const Float64 & l, const Null & r)     const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return l < r; }
    bool operator() (const Float64 & l, const Int64 & r)    const { return l < r; }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l < r; }
    bool operator() (const Float64 & l, const String & r)   const { return true; }
    bool operator() (const Float64 & l, const Array & r)    const { return true; }

    bool operator() (const String & l, const Null & r)      const { return false; }
    bool operator() (const String & l, const UInt64 & r)    const { return false; }
    bool operator() (const String & l, const Int64 & r)     const { return false; }
    bool operator() (const String & l, const Float64 & r)   const { return false; }
    bool operator() (const String & l, const String & r)    const { return l < r; }
    bool operator() (const String & l, const Array & r)     const { return true; }

    bool operator() (const Array & l, const Null & r)       const { return false; }
    bool operator() (const Array & l, const UInt64 & r)     const { return false; }
    bool operator() (const Array & l, const Int64 & r)      const { return false; }
    bool operator() (const Array & l, const Float64 & r)    const { return false; }
    bool operator() (const Array & l, const String & r)     const { return false; }
    bool operator() (const Array & l, const Array & r)      const { return l < r; }
};

#pragma GCC diagnostic pop

/** Диапазон с открытыми или закрытыми концами; возможно, неограниченный.
  */
struct Range
{
private:
	static bool equals(const Field & lhs, const Field & rhs) { return apply_visitor(FieldVisitorAccurateEquals(), lhs, rhs); }
	static bool less(const Field & lhs, const Field & rhs) { return apply_visitor(FieldVisitorAccurateLess(), lhs, rhs); }

public:
	Field left;				/// левая граница, если есть
	Field right;			/// правая граница, если есть
	bool left_bounded;		/// ограничен ли слева
	bool right_bounded; 	/// ограничен ли справа
	bool left_included; 	/// включает левую границу, если есть
	bool right_included;	/// включает правую границу, если есть

	/// Всё множество.
	Range() : left(), right(), left_bounded(false), right_bounded(false), left_included(false), right_included(false) {}

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
public:
	/// Не учитывает секцию SAMPLE. all_columns - набор всех столбцов таблицы.
	PKCondition(ASTPtr query, const Context & context, const NamesAndTypesList & all_columns, const SortDescription & sort_descr);

	/// Выполнимо ли условие в диапазоне ключей.
	/// left_pk и right_pk должны содержать все поля из sort_descr в соответствующем порядке.
	bool mayBeTrueInRange(const Field * left_pk, const Field * right_pk) const;

	/// Выполнимо ли условие в полубесконечном (не ограниченном справа) диапазоне ключей.
	/// left_pk должен содержать все поля из sort_descr в соответствующем порядке.
	bool mayBeTrueAfter(const Field * left_pk) const;

	/// Проверяет, что индекс не может быть использован.
	bool alwaysUnknown() const;

	/// Наложить дополнительное условие: значение в столбце column должно быть в диапазоне range.
	/// Возвращает, есть ли такой столбец в первичном ключе.
	bool addCondition(const String & column, const Range & range);

	String toString() const;
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

		Function function;

		/// Для FUNCTION_IN_RANGE и FUNCTION_NOT_IN_RANGE.
		Range range;
		size_t key_column;
		/// Для FUNCTION_IN_SET
		ASTPtr in_function;

		const ASTSet * inFunctionToSet() const;
	};

	typedef std::vector<RPNElement> RPN;
	typedef std::map<String, size_t> ColumnIndices;

	bool mayBeTrueInRange(const Field * left_pk, const Field * right_pk, bool right_bounded) const;

	void traverseAST(ASTPtr & node, Block & block_with_constants);
	bool atomFromAST(ASTPtr & node, Block & block_with_constants, RPNElement & out);
	bool operatorFromAST(ASTFunction * func, RPNElement & out);

	RPN rpn;

	SortDescription sort_descr;
	ColumnIndices pk_columns;
};

}
