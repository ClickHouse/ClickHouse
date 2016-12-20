#pragma once

#include <DB/Columns/IColumn.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

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

	bool operator==(const ColumnWithTypeAndName & other) const
	{
		return name == other.name
			&& ((!type && !other.type) || (type && other.type && type->getName() == other.type->getName()))
			&& ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
	}

	std::string prettyPrint() const
	{
		std::stringstream str;
		str << name << ' ';
		if (type)
			str << type->getName() << ' ';
		if (column)
			str << column->getName();
		return str.str();
	}
};

}
