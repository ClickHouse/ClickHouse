#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Storages/IStorage.h>


namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;


/** Общая часть StorageSet и StorageJoin.
  */
class StorageSetOrJoinBase : private ext::shared_ptr_helper<StorageSetOrJoinBase>, public IStorage
{
	friend class ext::shared_ptr_helper<StorageSetOrJoinBase>;
	friend class SetOrJoinBlockOutputStream;

public:
	String getTableName() const override { return name; }
	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

protected:
	StorageSetOrJoinBase(
		const String & path_,
		const String & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

	String path;
	String name;
	NamesAndTypesListPtr columns;

	UInt64 increment = 0;	/// Для имён файлов бэкапа.

	/// Восстановление из бэкапа.
	void restore();

private:
	void restoreFromFile(const String & file_path);

	/// Вставить блок в состояние.
	virtual void insertBlock(const Block & block) = 0;
	virtual size_t getSize() const = 0;
};


/** Позволяет сохранить множество для последующего использования в правой части оператора IN.
  * При вставке в таблицу, данные будут вставлены в множество,
  *  а также записаны в файл-бэкап, для восстановления после перезапуска.
  * Чтение из таблицы напрямую невозможно - возможно лишь указание в правой части оператора IN.
  */
class StorageSet : private ext::shared_ptr_helper<StorageSet>, public StorageSetOrJoinBase
{
friend class ext::shared_ptr_helper<StorageSet>;

public:
	static StoragePtr create(
		const String & path_,
		const String & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_)
	{
		return ext::shared_ptr_helper<StorageSet>::make_shared(path_, name_, columns_, materialized_columns_, alias_columns_, column_defaults_);
	}

	String getName() const override { return "Set"; }

	/// Получить доступ к внутренностям.
	SetPtr & getSet() { return set; }

private:
	SetPtr set;

	StorageSet(
		const String & path_,
		const String & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

	void insertBlock(const Block & block) override;
	size_t getSize() const override;
};

}
