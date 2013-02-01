#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


class NumbersBlockInputStream : public IProfilingBlockInputStream
{
public:
	NumbersBlockInputStream(size_t block_size_);
	String getName() const { return "NumbersBlockInputStream"; }
	BlockInputStreamPtr clone() { return new NumbersBlockInputStream(block_size); }
protected:
	Block readImpl();
private:
	size_t block_size;
	UInt64 next;
};


/** Реализует хранилище для системной таблицы Numbers.
  * Таблица содержит единственный столбец number UInt64.
  * Из этой таблицы можно прочитать все натуральные числа, начиная с 0 (до 2^64 - 1, а потом заново).
  */
class StorageSystemNumbers : public IStorage
{
public:
	StorageSystemNumbers(const std::string & name_);
	
	std::string getName() const { return "SystemNumbers"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

private:
	const std::string name;
	NamesAndTypesList columns;
};

}
