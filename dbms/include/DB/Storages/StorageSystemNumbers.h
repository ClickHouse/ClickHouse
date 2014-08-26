#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Storages/IStorage.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует хранилище для системной таблицы Numbers.
  * Таблица содержит единственный столбец number UInt64.
  * Из этой таблицы можно прочитать все натуральные числа, начиная с 0 (до 2^64 - 1, а потом заново).
  */
class StorageSystemNumbers : public IStorage
{
public:
	static StoragePtr create(const std::string & name_, bool multithreaded_ = false);
	
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
	bool multithreaded;
	
	StorageSystemNumbers(const std::string & name_, bool multithreaded_);
};

}
