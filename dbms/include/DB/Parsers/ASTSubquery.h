#pragma once

#include <DB/DataTypes/IDataType.h>

#include <DB/Parsers/IAST.h>


namespace DB
{


/** Подзарос SELECT в секции IN.
  */
class ASTSubquery : public IAST
{
public:
	/// находится ли внутри функции in
	bool is_in;
	/// тип возвращаемого значения
	DataTypePtr return_type;
	/// номер столбца возвращаемого значения
	size_t return_column_number;
	
	ASTSubquery() {}
	ASTSubquery(StringRange range_) : IAST(range_), is_in(false), return_column_number(0) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Subquery"; };

	ASTPtr clone() const { return new ASTSubquery(*this); }
};

}
