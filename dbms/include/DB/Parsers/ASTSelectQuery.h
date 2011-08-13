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
	ASTPtr select, from, where, group, having, order, limit;

	ASTSelectQuery() {}
	ASTSelectQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "SelectQuery"; };
};

}

#endif
