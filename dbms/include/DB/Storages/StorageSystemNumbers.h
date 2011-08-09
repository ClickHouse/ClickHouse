#ifndef DBMS_STORAGES_STORAGE_SYSTEM_NUMBERS_H
#define DBMS_STORAGES_STORAGE_SYSTEM_NUMBERS_H

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
	std::string getName() const { return "SystemNumbers"; }

	SharedPtr<IBlockInputStream> read(
		const Names & column_names,
		const ptree & query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE);
};

}

#endif
