#pragma once

#include <sstream>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Expression.h>
#include <DB/Storages/IStorage.h>
#include <DB/Core/SortDescription.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>


namespace DB
{
	
/** Диапазон с открытыми или закрытыми концами; возможно, неограниченный.
	*/
struct Range
{
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
	
	static Range RightBounded(const Field & right_point, bool right_included)
	{
		Range r;
		r.right = right_point;
		r.right_bounded = true;
		r.right_included = right_included;
		return r;
	}
	
	static Range LeftBounded(const Field & left_point, bool left_included)
	{
		Range r;
		r.left= left_point;
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
	bool contains(const Field & x)
	{
		return !leftThan(x) && !rightThan(x);
	}
	
	/// x находится левее
	bool rightThan(const Field & x)
	{
		return (left_bounded
		? !(boost::apply_visitor(FieldVisitorGreater(), x, left) || (left_included && x == left))
		: false);
	}
	
	/// x находится правее
	bool leftThan(const Field & x)
	{
		return (right_bounded
		? !(boost::apply_visitor(FieldVisitorLess(), x, right) || (right_included && x == right))
		: false);
	}
	
	bool intersectsRange(const Range & r)
	{
		/// r левее меня.
		if (r.right_bounded &&
			left_bounded &&
			(boost::apply_visitor(FieldVisitorLess(), r.right, left) ||
			((!left_included || !r.right_included) &&
			 r.right == left)))
			return false;
		/// r правее меня.
		if (r.left_bounded &&
			right_bounded &&
			(boost::apply_visitor(FieldVisitorGreater(), r.left, right) ||
			((!right_included || !r.left_included) &&
			r.left== right)))
			return false;
		return true;
	}
	
	bool containsRange(const Range & r)
	{
		/// r начинается левее меня.
		if (left_bounded &&
			(!r.left_bounded ||
			boost::apply_visitor(FieldVisitorLess(), r.left, left) ||
			(r.left_included &&
			!left_included &&
			r.left == left)))
			return false;
		/// r заканчивается правее меня.
		if (right_bounded &&
			(!r.right_bounded ||
			boost::apply_visitor(FieldVisitorGreater(), r.right, right) ||
			(r.right_included &&
			!right_included &&
			r.right == right)))
			return false;
		return true;
	}
	
	String toString()
	{
		std::stringstream str;
		
		if (!left_bounded)
			str << "(-inf, ";
		else
			str << (left_included ? '[' : '(') << boost::apply_visitor(FieldVisitorToString(), left) << ", ";
		
		if (!right_bounded)
			str << "+inf)";
		else
			str << boost::apply_visitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');
		
		return str.str();
	}
};


class PkCondition
{
public:
	PkCondition(ASTPtr query, const Context & context, const SortDescription & sort_descr);
	
	/// Выполнимо ли условие в диапазоне ключей.
	/// left_pk и right_pk должны содержать все поля из sort_descr в соответствующем порядке.
	bool mayBeTrueInRange(const Row & left_pk, const Row & right_pk);
	
	/// Выполнимо ли условие в полубесконечном (не ограниченном справа) диапазоне ключей.
	/// left_pk должен содержать все поля из sort_descr в соответствующем порядке.
	bool mayBeTrueAfter(const Row & left_pk);
	
	bool alwaysTrue()
	{
		return rpn.size() == 1 && rpn[0].function == RPNElement::FUNCTION_UNKNOWN;
	}
	
	String toString();
private:
	struct RPNElement
	{
		enum Function
		{
			/// Атомы логического выражения.
			FUNCTION_IN_RANGE,
			FUNCTION_NOT_IN_RANGE,
			FUNCTION_UNKNOWN, /// Может принимать любое значение.
			/// Операторы логического выражения.
			FUNCTION_NOT,
			FUNCTION_AND,
			FUNCTION_OR,
		};
		
		RPNElement() {}
		RPNElement(Function function_) : function(function_) {}
		RPNElement(Function function_, size_t key_column_) : function(function_), key_column(key_column_) {}
		
		String toString()
		{
			switch (function)
			{
				case FUNCTION_AND:
					return "and";
				case FUNCTION_OR:
					return "or";
				case FUNCTION_NOT:
					return "not";
				case FUNCTION_UNKNOWN:
					return "unknown";
				case FUNCTION_IN_RANGE:
				case FUNCTION_NOT_IN_RANGE:
				{
					std::ostringstream ss;
					ss << "(column " << key_column << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString() << ")";
					return ss.str();
				}
				default:
					return "ERROR";
			}
		}
		
		Function function;
		
		/// Для FUNCTION_IN_RANGE и FUNCTION_NOT_IN_RANGE.
		Range range;
		size_t key_column;
	};
	
	typedef std::vector<RPNElement> RPN;
	typedef std::map<String, size_t> ColumnIndices;
	
	bool mayBeTrueInRange(const Row & left_pk, const Row & right_pk, bool right_bounded);
	
	void traverseAST(ASTPtr & node, Block & block_with_constants);
	bool atomFromAST(ASTPtr & node, Block & block_with_constants, RPNElement & out);
	bool operatorFromAST(ASTFunction * func, RPNElement & out);
	
	RPN rpn;
	
	Context context;
	SortDescription sort_descr;
	ColumnIndices pk_columns;
};

}
