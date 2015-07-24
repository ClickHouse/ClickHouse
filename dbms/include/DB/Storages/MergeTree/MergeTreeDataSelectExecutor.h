#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeReader.h>


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
	  */
	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1,
		size_t * inout_part_index = nullptr);

private:
	MergeTreeData & data;

	Logger * log;

	struct RangesInDataPart
	{
		MergeTreeData::DataPartPtr data_part;
		size_t part_index_in_query;
		MarkRanges ranges;

		RangesInDataPart() {}

		RangesInDataPart(MergeTreeData::DataPartPtr data_part_, size_t part_index_in_query_)
			: data_part(data_part_), part_index_in_query(part_index_in_query_)
		{
		}
	};

	typedef std::vector<RangesInDataPart> RangesInDataParts;

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
