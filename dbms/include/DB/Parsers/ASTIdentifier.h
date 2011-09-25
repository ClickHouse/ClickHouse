#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Идентификатор (столбца или алиас, или именованый элемент кортежа)
  */
class ASTIdentifier : public IAST
{
public:
	enum Kind
	{
		Column,
		Database,
		Table,
	};
	
	/// имя
	String name;

	/// чего идентифицирует этот идентификатор
	Kind kind;

	/// тип (только для столбцов)
	DataTypePtr type;

	ASTIdentifier() {}
	ASTIdentifier(StringRange range_, const String & name_, Kind kind_ = Column) : IAST(range_), name(name_), kind(kind_) {}

	String getColumnName() { return name; }
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Identifier_" + name; }
};

}
