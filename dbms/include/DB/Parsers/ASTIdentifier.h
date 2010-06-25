#ifndef DBMS_PARSERS_ASTIDENTIFIER_H
#define DBMS_PARSERS_ASTIDENTIFIER_H

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Идентификатор (столбца или алиас, или именованый элемент кортежа)
  */
class ASTIdentifier : public IAST
{
public:
	StringRange range;
	/// имя
	String name;

	ASTIdentifier() {}
	ASTIdentifier(StringRange range_, const String & name_) : range(range_), name(name_) {}
	
	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }
};

}

#endif
