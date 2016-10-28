#include <DB/Storages/StorageFile.h>

#include <DB/DataStreams/FormatFactory.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>

#include <DB/Common/escapeForFileName.h>

#include <fcntl.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
	extern const int CANNOT_SEEK_THROUGH_FILE;
};


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
		use_table_fd = false;

		if (!table_path_.empty()) // Is user's file
		{
			path = Poco::Path(table_path_).absolute().toString();
			is_db_table = false;
		}
		else // Is DB's file
		{
			path = getTablePath(db_dir_path, table_name, format_name);
			is_db_table = true;
			Poco::File(Poco::Path(path).parent()).createDirectories();
		}
	}
	else
	{
		is_db_table = false;
		use_table_fd = true;
		table_fd_init_offset = lseek(table_fd, 0, SEEK_CUR);
	}
}


class StorageFileBlockInputStream : public IProfilingBlockInputStream
{
public:

	StorageFileBlockInputStream(StorageFile & storage_, const Context & context, size_t max_block_size)
	: storage(storage_), lock(storage.rwlock, storage.use_table_fd)
	{
		if (storage.use_table_fd)
		{
			/// We could use common ReadBuffer and WriteBuffer in storage to leverage cache
			///  and add ability to seek unseekable files, but cache sync isn't supported.

			if (storage.table_fd_was_used) /// We need seek to initial position
			{
				if (storage.table_fd_init_offset < 0)
					throw Exception("File descriptor isn't seekable, inside " + storage.getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

				/// ReadBuffer's seek() doesn't make sence, since cache is empty
				if (lseek(storage.table_fd, storage.table_fd_init_offset, SEEK_SET) < 0)
					throw Exception("Cannot seek file descriptor, inside " + storage.getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
			}

			storage.table_fd_was_used = true;
			read_buf = std::make_unique<ReadBufferFromFileDescriptor>(storage.table_fd);
		}
		else
		{
			read_buf = std::make_unique<ReadBufferFromFile>(storage.path);
		}

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
	Poco::ScopedRWLock lock;
	Block sample_block;
	std::unique_ptr<ReadBufferFromFileDescriptor> read_buf;
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
		if (storage.use_table_fd)
		{
			/// TODO: more detail checks: 1) fd is writeable 2) points to the end of data (if possible)
			if (storage.table_fd_was_used)
				throw Exception("Write to file descriptor after use, inside " + storage.getName(),
								ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);

			storage.table_fd_was_used = true;
			write_buf = std::make_unique<WriteBufferFromFileDescriptor>(storage.table_fd);
		}
		else
		{
			write_buf = std::make_unique<WriteBufferFromFile>(storage.path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
		}

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
	std::unique_ptr<WriteBufferFromFileDescriptor> write_buf;
	BlockOutputStreamPtr output_formatter;
};

BlockOutputStreamPtr StorageFile::write(
	ASTPtr query,
	const Settings & settings)
{
	return std::make_shared<StorageFileBlockOutputStream>(*this);
}


void StorageFile::drop()
{
	/// Extra actions are not required.
}


void StorageFile::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	if (!is_db_table)
		throw Exception("Can't rename table '" + table_name + "' stored in user-defined file (or FD)");

	Poco::ScopedWriteRWLock lock(rwlock);

	std::string path_new = getTablePath(new_path_to_db, new_table_name, format_name);
	Poco::File(Poco::Path(path_new).parent()).createDirectories();
	Poco::File(path).renameTo(path_new);

	path = std::move(path_new);
}

}
