#include <DB/Storages/StorageMergeTree.h>

namespace DB
{

StorageMergeTree::StorageMergeTree(const String & path_, const String & name_, NamesAndTypesListPtr columns_,
				const Context & context_,
				ASTPtr & primary_expr_ast_,
				const String & date_column_name_,
				const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
				size_t index_granularity_,
				MergeTreeData::Mode mode_,
				const String & sign_column_,
				const MergeTreeSettings & settings_)
	: data(	thisPtr(), path_, name_, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
			index_granularity_,mode_, sign_column_, settings_) {}

StoragePtr StorageMergeTree::create(
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
{
	return (new StorageMergeTree(
		path_, name_, columns_, context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, mode_, sign_column_, settings_))->thisPtr();
}

void StorageMergeTree::shutdown()
{
	data.shutdown();
}

StorageMergeTree::~StorageMergeTree() {}

BlockInputStreams StorageMergeTree::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	return data.read(column_names, query, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageMergeTree::write(ASTPtr query)
{
	return data.write(query);
}

void StorageMergeTree::dropImpl()
{
	data.dropImpl();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & new_name)
{
	data.rename(new_path_to_db, new_name);
}

void StorageMergeTree::alter(const ASTAlterQuery::Parameters & params)
{
	data.alter(params);
}

}
