#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/RangesInDataPart.h>
#include <DB/Storages/MergeTree/PKCondition.h>


namespace DB
{


/** Выполняет запросы SELECT на данных из merge-дерева.
  */
class MergeTreeDataSelectExecutor
{
public:
	MergeTreeDataSelectExecutor(MergeTreeData & data_);

	/** При чтении, выбирается набор кусков, покрывающий нужный диапазон индекса.
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
		size_t * inout_part_index,	/// Если не nullptr, из этого счетчика берутся значения для виртуального столбца _part_index.
		Int64 max_block_number_to_read) const;

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
		const Settings & settings) const;

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
		const Context & context) const;

	/// Получить приблизительное значение (оценку снизу - только по полным засечкам) количества строк, попадающего под индекс.
	size_t getApproximateTotalRowsToRead(
		const MergeTreeData::DataPartsVector & parts,
		const PKCondition & key_condition,
		const Settings & settings) const;

	/// Создать выражение "Sign == 1".
	void createPositiveSignCondition(
		ExpressionActionsPtr & out_expression,
		String & out_column,
		const Context & context) const;

	MarkRanges markRangesFromPKRange(
		const MergeTreeData::DataPart::Index & index,
		const PKCondition & key_condition,
		const Settings & settings) const;
};

}
