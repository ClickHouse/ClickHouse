#pragma once

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>

#include <DB/Columns/ColumnsNumber.h>

#include <DB/Interpreters/sortBlock.h>

#include <DB/Storages/MergeTree/MergeTreeData.h>


namespace DB
{

struct BlockWithDateInterval
{
	Block block;
	UInt16 min_date;
	UInt16 max_date;

	BlockWithDateInterval() : min_date(std::numeric_limits<UInt16>::max()), max_date(0) {}
	BlockWithDateInterval(const Block & block_, UInt16 min_date_, UInt16 max_date_)
	: block(block_), min_date(min_date_), max_date(max_date_) {}
};

typedef std::list<BlockWithDateInterval> BlocksWithDateIntervals;

/** Записывает новые куски с данными в merge-дерево.
  */
class MergeTreeDataWriter
{
public:
	MergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(&Logger::get(data.getLogName() + " (Writer)")) {}

	/** Разбивает блок на блоки, каждый из которых нужно записать в отдельный кусок.
	  *  (читай: разбивает строки по месяцам)
	  * Работает детерминированно: если отдать на вход такой же блок, на выходе получатся такие же блоки в таком же порядке.
	  */
	BlocksWithDateIntervals splitBlockIntoParts(const Block & block);

	/** Все строки должны относиться к одному месяцу.
	  * temp_index - значение left и right для нового куска. Можно будет изменить при переименовании.
	  * Возвращает кусок с именем, начинающимся с tmp_, еще не добавленный в MergeTreeData.
	  */
	MergeTreeData::MutableDataPartPtr writeTempPart(BlockWithDateInterval & block, Int64 temp_index);

private:
	MergeTreeData & data;

	Logger * log;
};

}
