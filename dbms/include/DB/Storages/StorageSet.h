#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Set.h>


namespace DB
{

/** Позволяет сохранить множество для последующего использования в правой части оператора IN.
  * При вставке в таблицу, данные будут вставлены в множество,
  *  а также записаны в файл-бэкап, для восстановления после перезапуска.
  * Чтение из таблицы напрямую невозможно - возможно лишь указание в правой части оператора IN.
  */
class StorageSet : public IStorage
{
public:
	static StoragePtr create(
		const String & path_,
		const String & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_)
	{
		return (new StorageSet{
			path_, name_, columns_,
			materialized_columns_, alias_columns_, column_defaults_})->thisPtr();
	}

	String getName() const override { return "Set"; }
	String getTableName() const override { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

	BlockOutputStreamPtr write(ASTPtr query) override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	/// Получить доступ к внутренностям.
	SetPtr & getSet() { return set; }

private:
	String path;
	String name;
	NamesAndTypesListPtr columns;

	UInt64 increment = 0;	/// Для имён файлов бэкапа.
	SetPtr set { new Set{Limits{}} };

	StorageSet(
		const String & path_,
		const String & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

	/// Восстановление из бэкапа.
	void restore();
	void restoreFromFile(const String & file_path, const DataTypeFactory & data_type_factory);
};

}
