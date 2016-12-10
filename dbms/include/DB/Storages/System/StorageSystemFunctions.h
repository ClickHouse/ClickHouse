#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <DB/Storages/IStorage.h>


namespace DB
{

class Context;


/** Реализует системную таблицу functions, которая позволяет получить список
  * всех обычных и агрегатных функций.
  */
class StorageSystemFunctions : private ext::shared_ptr_helper<StorageSystemFunctions>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemFunctions>;

public:
	static StoragePtr create(const std::string & name_);

	std::string getName() const override { return "SystemFunctions"; }
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
	StorageSystemFunctions(const std::string & name_);

private:
	const std::string name;
	NamesAndTypesList columns;
};

}
