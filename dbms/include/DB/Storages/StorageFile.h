#pragma once

#include <DB/Storages/IStorage.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/RWLock.h>

#include <common/logger_useful.h>

namespace DB
{

class StorageFileBlockInputStream;
class StorageFileBlockOutputStream;

class StorageFile : public IStorage
{
public:

	/** there are three options (ordered by priority):
		- use specified file descriptor if (fd >= 0)
		- use specified table_path if it isn't empty
		- create own tabale inside data/db/table/
	*/
	StorageFile(
		const std::string & table_path_,
		int table_fd_,
		const std::string & db_dir_path,
		const std::string & table_name_,
		const std::string & format_name_,
		const NamesAndTypesListPtr & columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		Context & context_);

	static StoragePtr create(
		const std::string & table_path,
		int table_fd,
		const std::string & db_dir_path,
		const std::string & table_name,
		const std::string & format_name,
		const NamesAndTypesListPtr & columns,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		Context & context)
	{
		return std::make_shared<StorageFile>(
			table_path, table_fd,
			db_dir_path, table_name, format_name, columns,
			materialized_columns_, alias_columns_, column_defaults_,
			context);
	}

	std::string getName() const override
	{
		return "File";
	}

	std::string getTableName() const override
	{
		return table_name;
	}

	const NamesAndTypesList & getColumnsListImpl() const override
	{
		return *columns;
	}

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockOutputStreamPtr write(
		ASTPtr query,
		const Settings & settings) override;

	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

protected:

	friend class StorageFileBlockInputStream;
	friend class StorageFileBlockOutputStream;

private:

	std::string table_name;
	std::string format_name;
	NamesAndTypesListPtr columns;
	Context & context_global;

	std::string path;
	int table_fd = -1;

	bool is_db_table = true; 					/// Table is stored in real database, not user's file
	bool use_table_fd = false;					/// Use table_fd insted of path
	std::atomic<bool> table_fd_was_used{false}; /// To detect repeating reads from stdin
	off_t table_fd_init_offset = -1;			/// Initial position of fd, used for repeating reads

	mutable Poco::RWLock rwlock;

	Logger * log = &Logger::get("StorageFile");
};

}
