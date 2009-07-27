#ifndef DBMS_COLUMN_GROUP_H
#define DBMS_COLUMN_GROUP_H

#include <list>
#include <string>

#include <Poco/SharedPtr.h>

#include <DB/Column.h>
#include <DB/Storage.h>


namespace DB
{

/** ColumnGroup - часть таблицы, которая хранится отдельно.
  * Может состоять из одного, нескольких или всех столбцов таблицы.
  * Таблица состоит из column-групп.
  */
struct ColumnGroup
{
	/// Номера столбцов
	typedef std::vector<size_t> ColumnNumbers;
	ColumnNumbers column_numbers;

	/// Хранилище
	Poco::SharedPtr<IStorage> storage;
};

}

#endif
