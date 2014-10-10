#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/Context.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует системную таблицу processes, которая позволяет получить информацию о запросах, исполняющихся в данный момент.
  */
class StorageSystemProcesses : public IStorage
{
public:
	static StoragePtr create(const std::string & name_, const Context & context_);

	std::string getName() const { return "SystemProcesses"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

private:
	const std::string name;
	const Context & context;
	NamesAndTypesList columns;

	StorageSystemProcesses(const std::string & name_, const Context & context_);
};

}
