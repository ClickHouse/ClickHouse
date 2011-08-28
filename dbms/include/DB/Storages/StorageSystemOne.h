#pragma once

#include <DB/Storages/IStorage.h>


namespace DB
{


class OneValueBlockInputStream : public IBlockInputStream
{
public:
	OneValueBlockInputStream();
	Block read();
private:
	bool has_been_read;
};


/** Реализует хранилище для системной таблицы One.
  * Таблица содержит единственный столбец dummy UInt8 и единственную строку со значением 0.
  * Используется, если в запросе не указана таблица.
  * Аналог таблицы DUAL в Oracle и MySQL.
  */
class StorageSystemOne : public IStorage
{
public:
	StorageSystemOne(const std::string & name_);
	
	std::string getName() const { return "SystemOne"; }
	std::string getTableName() const { return "One"; }

	const NamesAndTypes & getColumns() const { return columns; }

	BlockInputStreamPtr read(
		const Names & column_names,
		ASTPtr query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE);

private:
	const std::string name;
	NamesAndTypes columns;
};

}
