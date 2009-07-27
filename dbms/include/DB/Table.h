#ifndef DBMS_TABLE_H
#define DBMS_TABLE_H

#include <set>

#include <Poco/SharedPtr.h>

#include <DB/Column.h>
#include <DB/ColumnGroup.h>


namespace DB
{

class Table
{
friend class StorageNoKey;
friend class StorageNoKeyTablePartReader;
friend class StoragePlain;

public:
	typedef std::vector<Column> Columns;
	typedef std::vector<size_t> ColumnNumbers;
	typedef std::vector<ColumnGroup> ColumnGroups;

private:
	/// Имя таблицы
	std::string name;

	/// Столбцы
	Poco::SharedPtr<Columns> columns;

	/// Столбцы, которые относятся к первичному ключу
	Poco::SharedPtr<ColumnNumbers> primary_key_column_numbers;

	/// Группы столбцов
	Poco::SharedPtr<ColumnGroups> column_groups;

public:
	Table(const std::string & name_,
		const Poco::SharedPtr<Columns> columns_,
		const Poco::SharedPtr<ColumnNumbers> primary_key_column_numbers_,
		const Poco::SharedPtr<ColumnGroups> column_groups_)
		: name(name_),
		columns(columns_),
		primary_key_column_numbers(primary_key_column_numbers_),
		column_groups(column_groups_)
	{
		/// Пропишем в первичных ключах в кол-группах указатель на таблицу и кол-группу
		for (ColumnGroups::iterator it = column_groups->begin(); it != column_groups->end(); ++it)
			it->storage->addToTable(this, &*it);
	}
};

}

#endif
