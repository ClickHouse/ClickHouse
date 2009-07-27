#ifndef DBMS_STORAGE_H
#define DBMS_STORAGE_H

#include <Poco/SharedPtr.h>

#include <DB/RowSet.h>
#include <DB/TablePartReader.h>


namespace DB
{

class ColumnGroup;
class Table;

	
/** Хранилище - самая важная часть БД.
  * Отвечает за:
  * - хранение данных одной кол-группы таблицы
  * - определение, в каком файле (или не файле) хранятся данные;
  * - поиск данных и обновление данных;
  * - структура хранения данных (сжатие, etc.)
  * - конкуррентный доступ к данным (блокировки, etc.)
  * - реализует первичный ключ
  *
  * Присутствуют следующие особенности первичного ключа:
  * - может состоять из нескольких частей. Например, значения первых нескольких столбцов индексируются
  * с помощью файловой системы для удобства бэкапа и устаревания данных; а значения остальных столбцов
  * индексируются с помощью B-дерева.
  * - может не полностью индексировать столбцы, однозначно идентифицирующие строку в таблице -
  * для работы с пачками строк.
  */
class IStorage
{
friend class Table;

private:
	/** Установить указатель на таблицу и кол-группу.
	  * - часть инициализации, которая выполняется при инициализации таблицы.
	  * (инициализация хранилища выполняется в два шага:
	  * 1 - конструктор,
	  * 2 - добавление к таблице (выполняется в конструкторе Table))
	  */
	virtual void addToTable(Table * table_, ColumnGroup * column_group_) = 0;
	
public:
	/** Прочитать данные, соответствующие точному значению ключа или префиксу.
	  * Возвращает объект, с помощью которого можно последовательно читать данные.
	  */
	virtual Poco::SharedPtr<ITablePartReader> read(const Row & key) = 0;

	/** Записать пачку данных в таблицу, обновляя существующие данные, если они есть.
	  * @param data - набор данных вида ключ (набор столбцов) -> значение (набор столбцов)
	  * @param mask - битовая маска - какие столбцы входят в кол-группу,
	  * которую хранит это хранилище
	  */
	virtual void merge(const AggregatedRowSet & data, const ColumnMask & mask) = 0;

	virtual ~IStorage() {}
};


/** Реализует метод addToTable(),
  * а также содержит члены table, column_group.
  */
class StorageBase : public IStorage
{
protected:
	/// Слабые указатели на таблицу и column_group, которые владеют этим хранилищем.
	Table * table;
	ColumnGroup * column_group;

	StorageBase() : table(0), column_group(0) {}

	void addToTable(Table * table_, ColumnGroup * column_group_)
	{
		table = table_;
		column_group = column_group_;
	}
};

}

#endif
