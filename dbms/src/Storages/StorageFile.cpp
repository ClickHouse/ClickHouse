#include <DB/Storages/StorageFile.h>

#include <DB/DataStreams/FormatFactory.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Common/escapeForFileName.h>

namespace DB
{


static std::string getTablePath(const std::string & db_dir_path, const std::string & table_name, const std::string & format_name)
{
	return db_dir_path + escapeForFileName(table_name) + "/main." + escapeForFileName(format_name);
}


StorageFile::StorageFile(
		const std::string & table_path_,
		int table_fd_,
		const std::string & db_dir_path,
		const std::string & table_name_,
		const std::string & format_name_,
		const NamesAndTypesListPtr & columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		Context & context_)
	: IStorage(materialized_columns_, alias_columns_, column_defaults_),
	table_name(table_name_), format_name(format_name_), columns(columns_), context_global(context_), table_fd(table_fd_)
{
	if (table_fd < 0) // Will use file
	{
		if (!table_path_.empty()) // Is user file
		{
			path = Poco::Path(table_path_).absolute().toString();
			is_db_table = false;
		}
		else
		{
			path = getTablePath(db_dir_path, table_name, format_name);
			is_db_table = true;
			Poco::File(Poco::Path(path).parent()).createDirectories();
		}
	}
	else
	{
		is_db_table = false;
	}

	LOG_DEBUG(log, "Creating StorageFile " << table_name <<  " with format " << format_name << " using path " << path << " and dir " << db_dir_path);
}


class StorageFileBlockInputStream : public IProfilingBlockInputStream
{
public:

	StorageFileBlockInputStream(StorageFile & storage_, const Context & context, size_t max_block_size)
	: storage(storage_), lock(storage.rwlock)
	{
		read_buf = (!storage.path.empty()) ?
			std::make_unique<ReadBufferFromFile>(storage.path) :
			std::make_unique<ReadBufferFromFile>(storage.table_fd);

		input_formatter = FormatFactory().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
	}

	String getName() const override
	{
		return storage.getName();
	}

	String getID() const override
	{
		std::stringstream res_stream;
		res_stream << "File(" << &storage << ")";
		return res_stream.str();
	}

	Block readImpl() override
	{
		return input_formatter->read();
	}

	void readPrefixImpl() override
	{
		input_formatter->readPrefix();
	}

	void readSuffixImpl() override
	{
		input_formatter->readSuffix();
	}

private:
	StorageFile & storage;
	Poco::ScopedReadRWLock lock;
	Block sample_block;
	std::unique_ptr<ReadBufferFromFile> read_buf;
	BlockInputStreamPtr input_formatter;
};


BlockInputStreams StorageFile::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	return BlockInputStreams(1, std::make_shared<StorageFileBlockInputStream>(*this, context, max_block_size));
}


class StorageFileBlockOutputStream : public IBlockOutputStream
{
public:

	StorageFileBlockOutputStream(StorageFile & storage_)
	: storage(storage_), lock(storage.rwlock)
	{
		/// TODO: check that storage.table_fd is writeable

		write_buf = (!storage.path.empty()) ?
			 std::make_unique<WriteBufferFromFile>(storage.path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT) :
			 std::make_unique<WriteBufferFromFile>(storage.table_fd);

		output_formatter = FormatFactory().getOutput(storage.format_name, *write_buf, storage.getSampleBlock(), storage.context_global);
	}

	void write(const Block & block) override
	{
		output_formatter->write(block);
	}

	void writePrefix() override
	{
		output_formatter->writePrefix();
	}

	void writeSuffix() override
	{
		output_formatter->writeSuffix();
	}

	void flush() override
	{
		output_formatter->flush();
	}

private:
	StorageFile & storage;
	Poco::ScopedWriteRWLock lock;
	std::unique_ptr<WriteBufferFromFile> write_buf;
	BlockOutputStreamPtr output_formatter;
};

BlockOutputStreamPtr StorageFile::write(
	ASTPtr query,
	const Settings & settings)
{
	/// TODO: replace settings to context globally
	/// TODO: currently, for example, output_format_json_quote_64bit_integers works time-to-time

	return std::make_shared<StorageFileBlockOutputStream>(*this);
}


void StorageFile::drop()
{
	if (!is_db_table)
		throw Exception("Can't drop table '" + table_name + "' stored in user-defined file");

// 	Poco::ScopedWriteRWLock lock(rwlock);
// 	Poco::File(path).remove();
}


void StorageFile::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	if (!is_db_table)
		throw Exception("Can't rename table '" + table_name + "' stored in user-defined file");

	Poco::ScopedWriteRWLock lock(rwlock);

	std::string path_new = getTablePath(new_path_to_db, new_table_name, format_name);
	Poco::File(Poco::Path(path_new).parent()).createDirectories();
	Poco::File(path).renameTo(path_new);

	path = std::move(path_new);
}

}
