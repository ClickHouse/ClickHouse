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
		Format,
	};
	
	/// имя
	String name;
	/// алиас, если есть
	String alias;

	/// чего идентифицирует этот идентификатор
	Kind kind;

	ASTIdentifier() {}
	ASTIdentifier(StringRange range_, const String & name_, Kind kind_ = Column) : IAST(range_), name(name_), kind(kind_) {}

	String getColumnName() const { return name; }

	String getAlias() const { return alias.empty() ? getColumnName() : alias; }
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "Identifier_" + name; }

	ASTPtr clone() const { return new ASTIdentifier(*this); }
};

}
