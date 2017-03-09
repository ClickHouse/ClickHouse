#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <DB/Storages/IStorage.h>


namespace DB
{

class Context;


class StorageSystemDictionaries : private ext::shared_ptr_helper<StorageSystemDictionaries>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemDictionaries>;

public:
	static StoragePtr create(const std::string & name);

	std::string getName() const override { return "SystemDictionaries"; }
	std::string getTableName() const override { return name; }

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
	const NamesAndTypesList columns;

	StorageSystemDictionaries(const std::string & name);

	const NamesAndTypesList & getColumnsListImpl() const override { return columns; }
};

}
