#pragma once

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Interpreters/sortBlock.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Core/Block.h>

namespace DB
{

struct ShardedBlockWithDateInterval final
{
	ShardedBlockWithDateInterval(const Block & block_, size_t shard_no_, UInt16 min_date_, UInt16 max_date_);
	ShardedBlockWithDateInterval(const ShardedBlockWithDateInterval &) = delete;
	ShardedBlockWithDateInterval & operator=(const ShardedBlockWithDateInterval &) = delete;

	Block block;
	size_t shard_no;
	UInt16 min_date;
	UInt16 max_date;
};

using ShardedBlocksWithDateIntervals = std::list<ShardedBlockWithDateInterval>;

struct ReshardingJob;

/** Создаёт новые шардированные куски с данными.
  */
class MergeTreeSharder final
{
public:
	MergeTreeSharder(MergeTreeData & data_, const ReshardingJob & job_);
	MergeTreeSharder(const MergeTreeSharder &) = delete;
	MergeTreeSharder & operator=(const MergeTreeSharder &) = delete;

	/** Разбивает блок на блоки по ключу шардирования, каждый из которых
	  * нужно записать в отдельный кусок. Работает детерминированно: если
	  * отдать на вход такой же блок, на выходе получатся такие же блоки в
	  * таком же порядке.
	  */
	ShardedBlocksWithDateIntervals shardBlock(const Block & block);

private:
	std::vector<IColumn::Filter> createFilters(Block block);

private:
	MergeTreeData & data;
	const ReshardingJob & job;
	Logger * log;
	std::vector<size_t> slots;
	ExpressionActionsPtr sharding_key_expr;
	std::string sharding_key_column_name;
};

}
