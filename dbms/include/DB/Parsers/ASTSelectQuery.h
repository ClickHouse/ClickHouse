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

	ASTSelectQuery(StringRange range_) : range(range_) {}
	
	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }
};

}

#endif
