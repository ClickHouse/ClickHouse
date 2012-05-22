#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/Context.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует системную таблицу tables, которая позволяет получить информацию о всех таблицах.
  */
class StorageSystemTables : public IStorage
{
public:
	StorageSystemTables(const std::string & name_, const Context & context_);
	
	std::string getName() const { return "SystemTables"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned max_threads = 1);

private:
	const std::string name;
	const Context & context;
	NamesAndTypesList columns;
};

}
