#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>


namespace DB
{

/** Реализует системную таблицу settings, которая позволяет получить информацию о текущих настройках.
  */
class StorageSystemSettings : private ext::shared_ptr_helper<StorageSystemSettings>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemSettings>;

public:
	static StoragePtr create(const std::string & name_);

	std::string getName() const override { return "SystemSettings"; }
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

	StorageSystemSettings(const std::string & name_);
};

}
