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
	static StoragePtr create(const std::string & name_);

	std::string getName() const override { return "SystemOne"; }
	std::string getTableName() const override { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

private:
	const std::string name;
	NamesAndTypesList columns;

	StorageSystemOne(const std::string & name_);
};

}
