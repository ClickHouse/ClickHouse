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
	/// тип возвращаемого значения
	DataTypePtr return_type;
	/// номер столбца возвращаемого значения
	size_t return_column_number;
	
	ASTSubquery() {}
	ASTSubquery(StringRange range_) : IAST(range_), return_column_number(0) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "Subquery"; };

	ASTPtr clone() const { return new ASTSubquery(*this); }

	String getColumnName() const { return getTreeID(); }
};

}
