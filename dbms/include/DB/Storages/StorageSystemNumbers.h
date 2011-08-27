#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Storages/IStorage.h>


namespace DB
{

using Poco::SharedPtr;


class NumbersBlockInputStream : public IBlockInputStream
{
public:
	NumbersBlockInputStream(size_t block_size_);
	Block read();
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
	std::string getTableName() const { return "Numbers"; }

	const NamesAndTypes & getColumns() const { return columns; }

	SharedPtr<IBlockInputStream> read(
		const Names & column_names,
		ASTPtr query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE);

private:
	const std::string name;
	NamesAndTypes columns;
};

}
