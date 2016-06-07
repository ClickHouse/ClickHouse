#pragma once

#include <list>
#include <set>
#include <sstream>
#include <iostream>
#include <set>

#include <Poco/SharedPtr.h>

#include <common/Common.h>

#include <DB/Core/Types.h>
#include <DB/Common/Exception.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/StringRange.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NOT_A_COLUMN;
	extern const int TOO_BIG_AST;
	extern const int TOO_DEEP_AST;
	extern const int UNKNOWN_TYPE_OF_AST_NODE;
	extern const int UNKNOWN_ELEMENT_IN_AST;
}

using Poco::SharedPtr;
using IdentifierNameSet = std::set<String>;


/** Элемент синтаксического дерева (в дальнейшем - направленного ациклического графа с элементами семантики)
  */
class IAST
{
public:
	using ASTs = std::vector<SharedPtr<IAST>>;
	ASTs children;
	StringRange range;

	/// Указатель на начало секции [NOT]IN или JOIN в которой включен этот узел,
	/// если имеется такая секция.
	IAST * enclosing_in_or_join = nullptr;

	/// Атрибуты, которые нужны для некоторых алгоритмов на синтаксических деревьях.
	using Attributes = UInt32;
	Attributes attributes = 0;

	/// Был ли узел посещён? (см. класс LogicalExpressionsOptimizer)
	static constexpr Attributes IsVisited = 1U;
	/// Был ли узел обработан? (см. класс InJoinSubqueriesPreprocessor)
	static constexpr Attributes IsPreprocessedForInJoinSubqueries = 1U << 1;
	/// Является ли узел секцией IN?
	static constexpr Attributes IsIn = 1U << 2;
	/// Является ли узел секцией NOT IN?
	static constexpr Attributes IsNotIn = 1U << 3;
	/// Является ли узел секцией JOIN?
	static constexpr Attributes IsJoin = 1U << 4;
	/// Имеет ли секция IN/NOT IN/JOIN атрибут GLOBAL?
	static constexpr Attributes IsGlobal = 1U << 5;

	/** Глубина одного узла N - это глубина того запроса SELECT, которому принадлежит N.
	 *  Дальше глубина одного запроса SELECT определяется следующим образом:
	 *  - если запрос Q корневой, то select_query_depth(Q) = 0
	 *  - если запрос S является непосредственным подзапросом одного запроса R,
	 *  то select_query_depth(S) = select_query_depth(R) + 1
	 */
	UInt32 select_query_depth = 0;

	/** Строка с полным запросом.
	  * Этот указатель не дает ее удалить, пока range в нее ссылается.
	  */
	StringPtr query_string;

	IAST() = default;
	IAST(const StringRange range_) : range(range_) {}
	virtual ~IAST() = default;

	/** Получить каноническое имя столбца, если элемент является столбцом */
	virtual String getColumnName() const { throw Exception("Trying to get name of not a column: " + getID(), ErrorCodes::NOT_A_COLUMN); }

	/** Получить алиас, если он есть, или каноническое имя столбца, если его нет. */
	virtual String getAliasOrColumnName() const { return getColumnName(); }

	/** Получить алиас, если он есть, или пустую строку, если его нет, или если элемент не поддерживает алиасы. */
	virtual String tryGetAlias() const { return String(); }

	/** Установить алиас. */
	virtual void setAlias(const String & to)
	{
		throw Exception("Can't set alias of " + getColumnName(), ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
	}

	/** Получить текст, который идентифицирует этот элемент. */
	virtual String getID() const = 0;

	/** Получить глубокую копию дерева. */
	virtual SharedPtr<IAST> clone() const = 0;

	/// Рекурсивно установить атрибуты в поддереве, корнем которого является текущий узел.
	void setAttributes(Attributes attributes_)
	{
		attributes |= attributes_;
		for (auto it : children)
			it->setAttributes(attributes_);
	}

	/** Получить текст, который идентифицирует этот элемент и всё поддерево.
	  * Обычно он содержит идентификатор элемента и getTreeID от всех детей.
	  */
	String getTreeID() const
	{
		std::stringstream s;
		s << getID();

		if (!children.empty())
		{
			s << "(";
			for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
			{
				if (it != children.begin())
					s << ", ";
				s << (*it)->getTreeID();
			}
			s << ")";
		}

		return s.str();
	}

	void dumpTree(std::ostream & ostr, size_t indent = 0) const
	{
		String indent_str(indent, '-');
		ostr << indent_str << getID() << ", " << this << std::endl;
		for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
			(*it)->dumpTree(ostr, indent + 1);
	}

	/** Проверить глубину дерева.
	  * Если задано max_depth и глубина больше - кинуть исключение.
	  * Возвращает глубину дерева.
	  */
	size_t checkDepth(size_t max_depth) const
	{
		return checkDepthImpl(max_depth, 0);
	}

	/** То же самое для общего количества элементов дерева.
	  */
	size_t checkSize(size_t max_size) const
	{
		size_t res = 1;
		for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
			res += (*it)->checkSize(max_size);

		if (res > max_size)
			throw Exception("AST is too big. Maximum: " + toString(max_size), ErrorCodes::TOO_BIG_AST);

		return res;
	}

	/**  Получить set из имен индентификаторов
	 */
	virtual void collectIdentifierNames(IdentifierNameSet & set) const
	{
		for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
			(*it)->collectIdentifierNames(set);
	}


	/// Преобразовать в строку.

	/// Настройки формата.
	struct FormatSettings
	{
		std::ostream & ostr;
		bool hilite;
		bool one_line;

		char nl_or_ws;

		FormatSettings(std::ostream & ostr_, bool hilite_, bool one_line_)
			: ostr(ostr_), hilite(hilite_), one_line(one_line_)
		{
			nl_or_ws = one_line ? ' ' : '\n';
		}
	};

	/// Состояние. Например, может запоминаться множество узлов, которых мы уже обошли.
	struct FormatState
	{
		/** Запрос SELECT, в котором найден алиас; идентификатор узла с таким алиасом.
		  * Нужно, чтобы когда узел встретился повторно, выводить только алиас.
		  */
		std::set<std::pair<const IAST *, std::string>> printed_asts_with_alias;
	};

	/// Состояние, которое копируется при форматировании каждого узла. Например, уровень вложенности.
	struct FormatStateStacked
	{
		UInt8 indent = 0;
		bool need_parens = false;
		const IAST * current_select = nullptr;
	};

	void format(const FormatSettings & settings) const
	{
		FormatState state;
		formatImpl(settings, state, FormatStateStacked());
	}

	virtual void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
	{
		throw Exception("Unknown element in AST: " + getID()
			+ ((range.first && (range.second > range.first))
				? " '" + std::string(range.first, range.second - range.first) + "'"
				: ""),
			ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
	}

	void writeAlias(const String & name, std::ostream & s, bool hilite) const;

protected:
	/// Для подсветки синтаксиса.
	static const char * hilite_keyword;
	static const char * hilite_identifier;
	static const char * hilite_function;
	static const char * hilite_operator;
	static const char * hilite_alias;
	static const char * hilite_none;

private:
	size_t checkDepthImpl(size_t max_depth, size_t level) const
	{
		size_t res = level + 1;
		for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
		{
			if (level >= max_depth)
				throw Exception("AST is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_AST);
			res = std::max(res, (*it)->checkDepthImpl(max_depth, level + 1));
		}

		return res;
	}
};


using ASTPtr = SharedPtr<IAST>;
using ASTs = std::vector<ASTPtr>;


/// Квотировать идентификатор обратными кавычками, если это требуется.
String backQuoteIfNeed(const String & x);


}
