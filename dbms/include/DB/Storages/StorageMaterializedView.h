#pragma once

#include <DB/Storages/StorageView.h>


namespace DB
{

class StorageMaterializedView : public StorageView {

public:
	static StoragePtr create(
		const String & table_name_,
		const String & database_name_,
		Context & context_,
		ASTPtr & query_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		bool attach_);

	std::string getName() const override { return "MaterializedView"; }
	std::string getInnerTableName() const { return  ".inner." + table_name; }
	StoragePtr getInnerTable() const { return context.getTable(database_name, getInnerTableName()); }

	NameAndTypePair getColumn(const String & column_name) const override;
	bool hasColumn(const String & column_name) const override;

	bool supportsSampling() const override 			{ return getInnerTable()->supportsSampling(); }
	bool supportsPrewhere() const override 			{ return getInnerTable()->supportsPrewhere(); }
	bool supportsFinal() const override 			{ return getInnerTable()->supportsFinal(); }
	bool supportsIndexForIn() const override 		{ return getInnerTable()->supportsIndexForIn(); }
	bool supportsParallelReplicas() const override 	{ return getInnerTable()->supportsParallelReplicas(); }

	BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;
	void drop() override;
	bool optimize(const Settings & settings) override;

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

private:
	StorageMaterializedView(
		const String & table_name_,
		const String & database_name_,
		Context & context_,
		ASTPtr & query_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		bool attach_);
};

}
