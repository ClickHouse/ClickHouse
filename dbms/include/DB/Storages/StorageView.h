#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

class StorageView : private ext::shared_ptr_helper<StorageView>, public IStorage
{
friend class ext::shared_ptr_helper<StorageView>;

public:
	static StoragePtr create(
		const String & table_name_,
		const String & database_name_,
		Context & context_,
		ASTPtr & query_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

	std::string getName() const override { return "View"; }
	std::string getTableName() const override { return table_name; }
	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
	ASTPtr getInnerQuery() const { return inner_query.clone(); };

	/// Пробрасывается внутрь запроса и решается на его уровне.
	bool supportsSampling() const override { return true; }
	bool supportsFinal() 	const override { return true; }

	bool supportsParallelReplicas() const override { return true; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	void drop() override;

protected:
	String select_database_name;
	String select_table_name;
	String table_name;
	String database_name;
	ASTSelectQuery inner_query;
	Context & context;
	NamesAndTypesListPtr columns;

	StorageView(
		const String & table_name_,
		const String & database_name_,
		Context & context_,
		ASTPtr & query_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_);

private:
	/// Достать из самого внутреннего подзапроса имя базы данных и таблицы: select_database_name, select_table_name.
	void extractDependentTable(const ASTSelectQuery & query);
};

}
