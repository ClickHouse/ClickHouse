#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Columns/IColumn.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

/** Тип данных для представления столбца вместе с его типом и именем в оперативке.
  */

struct ColumnWithTypeAndName
{
	ColumnPtr column;
	DataTypePtr type;
	String name;

	ColumnWithTypeAndName() {}
	ColumnWithTypeAndName(const ColumnPtr & column_, const DataTypePtr & type_, const String name_)
		: column(column_), type(type_), name(name_) {}

	ColumnWithTypeAndName cloneEmpty() const
	{
		ColumnWithTypeAndName res;

		res.name = name;
		res.type = type->clone();
		if (column)
			res.column = column->cloneEmpty();

		return res;
	}
};

}
