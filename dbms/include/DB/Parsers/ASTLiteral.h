#pragma once

#include <DB/Core/Field.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/Parsers/ASTWithAlias.h>


namespace DB
{

/** Литерал (атомарный) - число, строка, NULL
  */
class ASTLiteral : public ASTWithAlias
{
public:
	Field value;
	/// тип
	DataTypePtr type;

	ASTLiteral() {}
	ASTLiteral(StringRange range_, const Field & value_) : ASTWithAlias(range_), value(value_) {}

	String getColumnName() const { return apply_visitor(FieldVisitorToString(), value); }

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "Literal_" + apply_visitor(FieldVisitorDump(), value); }

	ASTPtr clone() const { return new ASTLiteral(*this); }
};

}
