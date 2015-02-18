#pragma once

#include <list>
#include <set>
#include <sstream>

#include <Poco/SharedPtr.h>

#include <Yandex/Common.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/StringRange.h>

#include <iostream>


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

}
