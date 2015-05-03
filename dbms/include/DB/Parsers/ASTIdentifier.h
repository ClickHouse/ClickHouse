#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Parsers/ASTWithAlias.h>


namespace DB
{

/** Идентификатор (столбца или алиас, или именованый элемент кортежа)
  */
class ASTIdentifier : public ASTWithAlias
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

	/// чего идентифицирует этот идентификатор
	Kind kind;

	ASTIdentifier() = default;
	ASTIdentifier(const StringRange range_, const String & name_, const Kind kind_ = Column)
		: ASTWithAlias(range_), name(name_), kind(kind_) {}

	String getColumnName() const override { return name; }

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Identifier_" + name; }

	ASTPtr clone() const override { return new ASTIdentifier(*this); }

	void collectIdentifierNames(IdentifierNameSet & set) const override
	{
		set.insert(name);
	}
};

}
