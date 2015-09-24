#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>

namespace DB
{

/** Реализует системную таблицу columns, которая позволяет получить информацию
  * о столбцах каждой таблицы для всех баз данных.
  */
class StorageSystemColumns : public IStorage
{
public:
	static StoragePtr create(const std::string & name_);

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
	StorageSystemColumns(const std::string & name_);

private:
	const std::string name;
	NamesAndTypesList columns;
};

}
