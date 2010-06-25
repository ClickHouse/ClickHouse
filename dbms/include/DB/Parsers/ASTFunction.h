#ifndef DBMS_PARSERS_ASTFUNCTION_H
#define DBMS_PARSERS_ASTFUNCTION_H

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Применение функции или оператора
  */
class ASTFunction : public IAST
{
public:
	StringRange range;
	/// имя функции
	String name;
	/// параметры
	ASTPtr arguments;

	ASTFunction() {}
	ASTFunction(StringRange range_) : range(range_) {}

	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }
};

}

#endif
