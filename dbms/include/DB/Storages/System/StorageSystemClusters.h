#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <DB/Storages/IStorage.h>


namespace DB
{

class Context;

/** Implements system table 'clusters'
  *  that allows to obtain information about available clusters
  *  (which may be specified in Distributed tables).
  */
class StorageSystemClusters : private ext::shared_ptr_helper<StorageSystemClusters>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemClusters>;

public:
	StorageSystemClusters(const std::string & name_, Context & context_);
	static StoragePtr create(const std::string & name_, Context & context_);

	std::string getName() const override { return "SystemClusters"; }
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
