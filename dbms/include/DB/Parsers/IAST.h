#pragma once

#include <list>
#include <set>
#include <sstream>

#include <Poco/SharedPtr.h>

#include <Yandex/Common.h>

#include <DB/Core/Types.h>
#include <DB/Common/UInt128.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Common/SipHash.h>
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

	/** Получить имя, однозначно идентифицирующее выражение, если элемент является столбцом. У одинаковых выражений будет одинаковое имя. */
	virtual String getColumnName() const
	{
		/// По-умолчанию - подчёркивание, а затем getTreeID в hex-е.

		union
		{
			UInt128 id;
			UInt8 id_bytes[16];
		};

		id = getTreeID();
		String res(1 + 2 * sizeof(id), '_');

		for (size_t i = 0; i < sizeof(id); ++i)
		{
			res[i * 2 + 1] = (id_bytes[i] / 16) < 10 ? ('0' + (id_bytes[i] / 16)) : ('A' + (id_bytes[i] / 16 - 10));
			res[i * 2 + 2] = (id_bytes[i] % 16) < 10 ? ('0' + (id_bytes[i] % 16)) : ('A' + (id_bytes[i] % 16 - 10));
		}

		return res;
	}

	/** Получить алиас, если он есть, или имя столбца, если его нет. */
	virtual String getAliasOrColumnName() const { return getColumnName(); }

	/** Получить алиас, если он есть, или пустую строку, если его нет, или если элемент не поддерживает алиасы. */
	virtual String tryGetAlias() const { return String(); }

	/** Обновить состояние хэш-функции элементом дерева. */
	virtual void updateHashWith(SipHash & hash) const = 0;

	/** Обновить состояние хэш-функции целым поддеревом. Используется для склейки одинаковых выражений. */
	void updateHashWithTree(SipHash & hash) const
	{
		updateHashWith(hash);

		if (!children.empty())
		{
			size_t size = children.size();
			hash.update(reinterpret_cast<const char *>(&size), sizeof(size));

			for (size_t i = 0; i < size; ++i)
			{
				hash.update(reinterpret_cast<const char *>(&i), sizeof(i));
				children[i]->updateHashWithTree(hash);
			}
		}
	}

	/** Получить идентификатор поддерева. Используется для склейки одинаковых выражений.
	  */
	UInt128 getTreeID() const
	{
		SipHash hash;
		updateHashWithTree(hash);
		UInt128 res;
		hash.get128(reinterpret_cast<char *>(&res));
		return res;
	}

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
		for (const auto & ast : children)
			res += ast->checkSize(max_size);

		if (res > max_size)
			throw Exception("AST is too big. Maximum: " + toString(max_size), ErrorCodes::TOO_BIG_AST);

		return res;
	}

	/**  Получить set из имен индентификаторов
	  */
	virtual void collectIdentifierNames(IdentifierNameSet & set) const
	{
		for (const auto & ast : children)
			ast->collectIdentifierNames(set);
	}

private:
	size_t checkDepthImpl(size_t max_depth, size_t level) const
	{
		size_t res = level + 1;
		for (const auto & ast : children)
		{
			if (level >= max_depth)
				throw Exception("AST is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_AST);
			res = std::max(res, ast->checkDepthImpl(max_depth, level + 1));
		}

		return res;
	}
};


typedef SharedPtr<IAST> ASTPtr;
typedef std::vector<ASTPtr> ASTs;

}
