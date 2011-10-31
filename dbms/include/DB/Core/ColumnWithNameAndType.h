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

	ColumnWithNameAndType cloneEmpty() const
	{
		ColumnWithNameAndType res;

		res.name = name;
		res.type = type;
		if (column)
			res.column = column->cloneEmpty();

		return res;
	}
};

}
