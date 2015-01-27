#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Join.h>


namespace DB
{

/** Позволяет сохранить состояние для последующего использования в правой части JOIN.
  * При вставке в таблицу, данные будут вставлены в состояние,
  *  а также записаны в файл-бэкап, для восстановления после перезапуска.
  * Чтение из таблицы напрямую невозможно - возможно лишь указание в правой части JOIN.
  *
  * NOTE: В основном, повторяет StorageSet. Можно обобщить.
  */
class StorageJoin : public IStorage
{
public:
	static StoragePtr create(
		const String & path_,
		const String & name_,
		const Names & key_names_,
		ASTJoin::Kind kind_, ASTJoin::Strictness strictness_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_)
	{
		return (new StorageJoin{
			path_, name_, columns_,
			materialized_columns_, alias_columns_, column_defaults_})->thisPtr();
	}

	String getName() const override { return "Join"; }
	String getTableName() const override { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

	BlockOutputStreamPtr write(ASTPtr query) override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	/// Получить доступ к внутренностям.
	JoinPtr & getJoin() { return join; }

private:
	String path;
	String name;
	NamesAndTypesListPtr columns;

	UInt64 increment = 0;	/// Для имён файлов бэкапа.
	JoinPtr join;

	StorageJoin(
		const String & path_,
		const String & name_,
		const Names & key_names_,
		ASTJoin::Kind kind_, ASTJoin::Strictness strictness_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

	/// Восстановление из бэкапа.
	void restore();
	void restoreFromFile(const String & file_path, const DataTypeFactory & data_type_factory);
};

}
