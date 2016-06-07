#pragma once

#include <DB/Storages/IStorage.h>

namespace DB
{

class Context;

/** Реализует системную таблицу columns, которая позволяет получить информацию
  * о столбцах каждой таблицы для всех баз данных.
  */
class StorageSystemClusters : public IStorage
{
public:
	StorageSystemClusters(const std::string & name_, Context & context_);
	static StoragePtr create(const std::string & name_, Context & context_);

	std::string getName() const override { return "SystemColumns"; }
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
	StorageSystemClusters(const std::string & name_);

private:
	const std::string name;
	NamesAndTypesList columns;
	Context & context;
};

}
