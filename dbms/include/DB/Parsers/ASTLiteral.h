#ifndef DBMS_PARSERS_ASTLITERAL_H
#define DBMS_PARSERS_ASTLITERAL_H

#include <DB/Core/Field.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Литерал (атомарный) - число, строка, NULL
  */
class ASTLiteral : public IAST
{
public:
	StringRange range;
	/// значение
	Field value;

	ASTLiteral() {}
	ASTLiteral(StringRange range_, const Field & value_) : range(range_), value(value_) {}
	
	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }
};

}

#endif
