#ifndef DBMS_PARSERS_IAST_H
#define DBMS_PARSERS_IAST_H

#include <list>
#include <Poco/SharedPtr.h>

#include <DB/Core/Types.h>
#include <DB/Parsers/StringRange.h>


namespace DB
{

using Poco::SharedPtr;


/** Элемент синтаксического дерева (в дальнейшем - направленного ациклического графа с элементами семантики)
  */
class IAST
{
public:
	/** Получить кусок текста, откуда был получен этот элемент. */
	virtual StringRange getRange() = 0;
	virtual ~IAST() {}
};


typedef Poco::SharedPtr<IAST> ASTPtr;
typedef std::list<ASTPtr> ASTs;

}

#endif
