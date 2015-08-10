#pragma once

#include <list>
#include <set>
#include <sstream>
#include <iostream>
#include <set>

#include <Poco/SharedPtr.h>

#include <Yandex/Common.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/StringRange.h>


namespace DB
{

using Poco::SharedPtr;
typedef std::set<String> IdentifierNameSet;


/** Элемент синтаксического дерева (в дальнейшем - направленного ациклического графа с элементами семантики)
  */
class IAST
{
public:
	typedef std::vector<SharedPtr<IAST> > ASTs;
	ASTs children;
	StringRange range;
	bool is_visited = false;

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

	void clearVisited()
	{
		is_visited = false;
		for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
			(*it)->is_visited = false;
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


typedef SharedPtr<IAST> ASTPtr;
typedef std::vector<ASTPtr> ASTs;


/// Квотировать идентификатор обратными кавычками, если это требуется.
String backQuoteIfNeed(const String & x);


}
