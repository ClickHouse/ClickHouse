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

	ASTLiteral() = default;
	ASTLiteral(const StringRange range_, const Field & value_) : ASTWithAlias(range_), value(value_) {}

	String getColumnName() const override { return apply_visitor(FieldVisitorToString(), value); }

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Literal_" + apply_visitor(FieldVisitorDump(), value); }

	ASTPtr clone() const override { return new ASTLiteral(*this); }
};

}
