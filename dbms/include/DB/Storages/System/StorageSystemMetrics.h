#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Storages/IStorage.h>


namespace DB
{

/** Реализует системную таблицу metrics, которая позволяет получить информацию о работе сервера.
	*/
class StorageSystemMetrics : private ext::shared_ptr_helper<StorageSystemMetrics>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemMetrics>;

public:
	static StoragePtr create(const std::string & name_);

	std::string getName() const override { return "SystemMetrics"; }
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

	StorageSystemMetrics(const std::string & name_);
};

}
