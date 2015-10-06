#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>
#include <DB/Storages/MergeTree/RangesInDataPart.h>


namespace DB
{


/** Выполняет запросы SELECT на данных из merge-дерева.
  */
class MergeTreeDataSelectExecutor
{
public:
	MergeTreeDataSelectExecutor(MergeTreeData & data_);

	/** При чтении, выбирается набор кусков, покрывающий нужный диапазон индекса.
	  * Если inout_part_index != nullptr, из этого счетчика берутся значения для виртуального столбца _part_index.
	  * max_block_number_to_read - если не ноль - не читать все куски, у которых правая граница больше этого порога.
	  */
	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size,
		unsigned threads,
		size_t * inout_part_index,
		Int64 max_block_number_to_read);

private:
	MergeTreeData & data;

	Logger * log;

	BlockInputStreams spreadMarkRangesAmongThreads(
		RangesInDataParts parts,
		size_t threads,
		const Names & column_names,
		size_t max_block_size,
		bool use_uncompressed_cache,
		ExpressionActionsPtr prewhere_actions,
		const String & prewhere_column,
		const Names & virt_columns,
		const Settings & settings);

	BlockInputStreams spreadMarkRangesAmongThreadsFinal(
		RangesInDataParts parts,
		size_t threads,
		const Names & column_names,
		size_t max_block_size,
		bool use_uncompressed_cache,
		ExpressionActionsPtr prewhere_actions,
		const String & prewhere_column,
		const Names & virt_columns,
		const Settings & settings,
		const Context & context);

	/// Создать выражение "Sign == 1".
	void createPositiveSignCondition(ExpressionActionsPtr & out_expression, String & out_column, const Context & context);

	MarkRanges markRangesFromPkRange(const MergeTreeData::DataPart::Index & index, PKCondition & key_condition, const Settings & settings);
};

}
