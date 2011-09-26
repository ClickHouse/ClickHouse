#pragma once

#include <DB/Core/Field.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Литерал (атомарный) - число, строка, NULL
  */
class ASTLiteral : public IAST
{
public:
	Field value;
	/// тип
	DataTypePtr type;

	ASTLiteral() {}
	ASTLiteral(StringRange range_, const Field & value_) : IAST(range_), value(value_) {}

	String getColumnName() { return boost::apply_visitor(FieldVisitorToString(), value); }
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Literal_" + boost::apply_visitor(FieldVisitorDump(), value); }
};

}
