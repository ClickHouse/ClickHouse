#ifndef DBMS_COLUMN_H
#define DBMS_COLUMN_H

#include <string>

#include <Poco/SharedPtr.h>

#include <DB/ColumnType.h>


namespace DB
{
	
/** Столбец - часть ColumnGroup, которая, в свою очередь - часть таблицы
  */
struct Column
{
	std::string name;
	Poco::SharedPtr<IColumnType> type;

	Column(const std::string & name_, Poco::SharedPtr<IColumnType> type_)
		: name(name_), type(type_) {}
};
	
}

#endif
