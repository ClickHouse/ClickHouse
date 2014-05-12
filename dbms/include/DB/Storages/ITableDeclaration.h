#pragma once

#include <DB/Core/Names.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Core/Exception.h>
#include <DB/Core/Block.h>
#include <DB/Parsers/ASTAlterQuery.h>

namespace DB
{

class Context;

/** Описание таблицы.
  * Не thread safe. См. IStorage::lockStructure().
  */
class ITableDeclaration
{
public:
	/** Имя таблицы.
	  */
	virtual std::string getTableName() const = 0;

	/** Получить список имён и типов столбцов таблицы, только невиртуальные.
	  */
	virtual const NamesAndTypesList & getColumnsList() const = 0;

	/** Получить список имён столбцов таблицы, только невиртуальные.
	  */
	virtual Names getColumnNamesList() const;

	/** Получить описание реального (невиртуального) столбца по его имени.
	  */
	virtual NameAndTypePair getRealColumn(const String & column_name) const;

	/** Присутствует ли реальный (невиртуальный) столбец с таким именем.
	  */
	virtual bool hasRealColumn(const String & column_name) const;

	/** Получить описание любого столбца по его имени.
	  */
	virtual NameAndTypePair getColumn(const String & column_name) const;

	/** Присутствует ли столбец с таким именем.
	  */
	virtual bool hasColumn(const String & column_name) const;

	const DataTypePtr getDataTypeByName(const String & column_name) const;

	/** То же самое, но в виде блока-образца.
	  */
	Block getSampleBlock() const;

	/** Проверить, что все запрошенные имена есть в таблице и заданы корректно.
	  * (список имён не пустой и имена не повторяются)
	  */
	void check(const Names & column_names) const;

	/** Проверить, что блок с данными для записи содержит все столбцы таблицы с правильными типами,
	  *  содержит только столбцы таблицы, и все столбцы различны.
	  * Если need_all, еще проверяет, что все столбцы таблицы есть в блоке.
	  */
	void check(const Block & block, bool need_all = false) const;

	/// реализация alter, модифицирующая список столбцов.
	static void alterColumns(const ASTAlterQuery::Parameters & params, NamesAndTypesListPtr & columns, const Context & context);

	virtual ~ITableDeclaration() {}
};

}
