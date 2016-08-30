#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Storages/IStorage.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

class DatabaseCloud;


/** Облачная таблица. Может находиться только в облачной базе данных.
  * При записи в таблицу, данные записываются в локальные таблицы на нескольких серверах облака.
  */
class StorageCloud : private ext::shared_ptr_helper<StorageCloud>, public IStorage
{
friend class ext::shared_ptr_helper<StorageCloud>;

public:
	static StoragePtr create(
		DatabasePtr & database_ptr_,
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

	std::string getName() const override { return "Cloud"; }
	std::string getTableName() const override { return name; }

	/// Проверка откладывается до метода read. Там проверяется поддержка у использующихся таблиц.
	bool supportsSampling() const override { return true; }
	bool supportsPrewhere() const override { return true; }
	bool supportsFinal() 	const override { return true; }

	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
	NameAndTypePair getColumn(const String & column_name) const override;
	bool hasColumn(const String & column_name) const override;

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	void drop() override {}		/// Вся нужная работа в DatabaseCloud::removeTable

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override
	{
		name = new_table_name;
	}

private:
	String name;
	NamesAndTypesListPtr columns;

	std::weak_ptr<IDatabase> database_ptr;

	StorageCloud(
		DatabasePtr & database_ptr_,
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);
};

}
