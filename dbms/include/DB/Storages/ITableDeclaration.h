#pragma once

#include <DB/Core/Names.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Common/Exception.h>
#include <DB/Core/Block.h>
#include <DB/Storages/ColumnDefault.h>

#include <boost/range/iterator_range.hpp>
#include <boost/range/join.hpp>


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
	NamesAndTypesList getColumnsList() const;
	const NamesAndTypesList & getColumnsListNonMaterialized() const { return getColumnsListImpl(); }

	/** Получить список имён столбцов таблицы, только невиртуальные.
	  */
	virtual Names getColumnNamesList() const;

	/** Получить описание реального (невиртуального) столбца по его имени.
	  */
	virtual NameAndTypePair getRealColumn(const String & column_name) const;

	/** Присутствует ли реальный (невиртуальный) столбец с таким именем.
	  */
	virtual bool hasRealColumn(const String & column_name) const;

	NameAndTypePair getMaterializedColumn(const String & column_name) const;
	bool hasMaterializedColumn(const String & column_name) const;

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
	Block getSampleBlockNonMaterialized() const;

	/** Проверить, что все запрошенные имена есть в таблице и заданы корректно.
	  * (список имён не пустой и имена не повторяются)
	  */
	void check(const Names & column_names) const;

	/** Проверить, что все запрошенные имена есть в таблице и имеют правильные типы.
	  */
	void check(const NamesAndTypesList & columns) const;

	/** Проверить, что все имена из пересечения names и columns есть в таблице и имеют одинаковые типы.
	  */
	void check(const NamesAndTypesList & columns, const Names & column_names) const;

	/** Проверить, что блок с данными для записи содержит все столбцы таблицы с правильными типами,
	  *  содержит только столбцы таблицы, и все столбцы различны.
	  * Если need_all, еще проверяет, что все столбцы таблицы есть в блоке.
	  */
	void check(const Block & block, bool need_all = false) const;


	virtual ~ITableDeclaration() = default;

	ITableDeclaration() = default;
	ITableDeclaration(
		const NamesAndTypesList & materialized_columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults)
		: materialized_columns{materialized_columns},
		  alias_columns{alias_columns},
		  column_defaults{column_defaults}
	{}

	NamesAndTypesList materialized_columns{};
	NamesAndTypesList alias_columns{};
	ColumnDefaults column_defaults{};

private:
	virtual const NamesAndTypesList & getColumnsListImpl() const = 0;

	using ColumnsListRange = boost::range::joined_range<const NamesAndTypesList, const NamesAndTypesList>;
	/// Returns a lazily joined range of table's ordinary and materialized columns, without unnecessary copying
	ColumnsListRange getColumnsListRange() const;
};

}
