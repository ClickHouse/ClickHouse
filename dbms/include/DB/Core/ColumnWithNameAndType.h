#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Columns/IColumn.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

/** Тип данных для представления столбца вместе с его типом и именем в оперативке.
  */

struct ColumnWithNameAndType
{
	ColumnPtr column;
	DataTypePtr type;
	String name;
	
	ColumnWithNameAndType() {}
	ColumnWithNameAndType(const ColumnPtr & column_, const DataTypePtr & type_, const String name_)
		: column(column_), type(type_), name(name_) {}

	ColumnWithNameAndType cloneEmpty() const
	{
		ColumnWithNameAndType res;

		res.name = name;
		res.type = type->clone();
		if (column)
			res.column = column->cloneEmpty();

		return res;
	}
};

}
