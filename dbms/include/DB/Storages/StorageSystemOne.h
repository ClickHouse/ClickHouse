#pragma once

#include <DB/Storages/IStorage.h>


namespace DB
{


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
