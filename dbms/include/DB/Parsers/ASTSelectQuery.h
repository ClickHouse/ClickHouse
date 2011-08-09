#ifndef DBMS_PARSERS_ASTSELECTQUERY_H
#define DBMS_PARSERS_ASTSELECTQUERY_H

#include <DB/Parsers/IAST.h>


namespace DB
{


/** SELECT запрос
  */
class ASTSelectQuery : public IAST
{
public:
	StringRange range;
	ASTPtr select, from, where, group, having, order, limit;

	ASTSelectQuery() {}
	ASTSelectQuery(StringRange range_) : range(range_) {}
	
	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }

	/** Получить всех детей. */
	ASTs getChildren()
	{
		ASTs res;
		if (!select.isNull())
			res.push_back(select);
		if (!from.isNull())
			res.push_back(from);
		if (!where.isNull())
			res.push_back(where);
		if (!group.isNull())
			res.push_back(group);
		if (!having.isNull())
			res.push_back(having);
		if (!order.isNull())
			res.push_back(order);
		if (!limit.isNull())
			res.push_back(limit);
		return res;
	}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "SelectQuery"; };
};

}

#endif
