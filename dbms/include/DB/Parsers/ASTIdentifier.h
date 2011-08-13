#ifndef DBMS_PARSERS_ASTIDENTIFIER_H
#define DBMS_PARSERS_ASTIDENTIFIER_H

#include <DB/DataTypes/IDataType.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Идентификатор (столбца или алиас, или именованый элемент кортежа)
  */
class ASTIdentifier : public IAST
{
public:
	/// имя
	String name;
	/// тип
	DataTypePtr type;

	ASTIdentifier() {}
	ASTIdentifier(StringRange range_, const String & name_) : IAST(range_), name(name_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Identifier_" + name; }
};

}

#endif
