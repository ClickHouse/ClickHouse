#include <Storages/StorageFile.h>

#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <DataStreams/FormatFactory.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Common/escapeForFileName.h>

#include <fcntl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int DATABASE_ACCESS_DENIED;
};


static std::string getTablePath(const std::string & db_dir_path, const std::string & table_name, const std::string & format_name)
{
    return db_dir_path + escapeForFileName(table_name) + "/data." + escapeForFileName(format_name);
}

static void checkCreationIsAllowed(Context & context_global)
{
    if (context_global.getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception("Using file descriptor or user specified path as source of storage isn't allowed for server daemons", ErrorCodes::DATABASE_ACCESS_DENIED);
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
    if (table_fd < 0) /// Will use file
    {
        use_table_fd = false;

        if (!table_path_.empty()) /// Is user's file
        {
            checkCreationIsAllowed(context_global);
            path = Poco::Path(table_path_).absolute().toString();
            is_db_table = false;
        }
        else /// Is DB's file
        {
            path = getTablePath(db_dir_path, table_name, format_name);
            is_db_table = true;
            Poco::File(Poco::Path(path).parent()).createDirectories();
        }
    }
    else /// Will use FD
    {
        checkCreationIsAllowed(context_global);
        is_db_table = false;
        use_table_fd = true;

        /// Save initial offset, it will be used for repeating SELECTs
        /// If FD isn't seekable (lseek returns -1), then the second and subsequent SELECTs will fail.
        table_fd_init_offset = lseek(table_fd, 0, SEEK_CUR);
    }
}


class StorageFileBlockInputStream : public IProfilingBlockInputStream
{
public:

    StorageFileBlockInputStream(StorageFile & storage_, const Context & context, size_t max_block_size)
        : storage(storage_)
    {
        if (storage.use_table_fd)
        {
            storage.rwlock.lock();

            /// We could use common ReadBuffer and WriteBuffer in storage to leverage cache
            ///  and add ability to seek unseekable files, but cache sync isn't supported.

            if (storage.table_fd_was_used) /// We need seek to initial position
            {
                if (storage.table_fd_init_offset < 0)
                    throw Exception("File descriptor isn't seekable, inside " + storage.getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

                /// ReadBuffer's seek() doesn't make sence, since cache is empty
                if (lseek(storage.table_fd, storage.table_fd_init_offset, SEEK_SET) < 0)
                    throwFromErrno("Cannot seek file descriptor, inside " + storage.getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
            }

            storage.table_fd_was_used = true;
            read_buf = std::make_unique<ReadBufferFromFileDescriptor>(storage.table_fd);
        }
        else
        {
            storage.rwlock.lock_shared();

            read_buf = std::make_unique<ReadBufferFromFile>(storage.path);
        }

        reader = FormatFactory().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    ~StorageFileBlockInputStream() override
    {
        if (storage.use_table_fd)
            storage.rwlock.unlock();
        else
            storage.rwlock.unlock_shared();
    }

    String getName() const override
    {
        return storage.getName();
    }

    String getID() const override
    {
        std::stringstream res_stream;
        res_stream << "File(" << storage.format_name << ", ";
        if (!storage.path.empty())
            res_stream << storage.path;
        else
            res_stream << storage.table_fd;
        res_stream << ")";
        return res_stream.str();
    }

    Block readImpl() override
    {
        return reader->read();
    }

    void readPrefixImpl() override
    {
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();
    }

private:
    StorageFile & storage;
    Block sample_block;
    std::unique_ptr<ReadBufferFromFileDescriptor> read_buf;
    BlockInputStreamPtr reader;
};


BlockInputStreams StorageFile::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
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
            /** NOTE: Using real file binded to FD may be misleading:
              * SELECT *; INSERT insert_data; SELECT *; last SELECT returns initil_fd_data + insert_data
              * INSERT data; SELECT *; last SELECT returns only insert_data
              */
            storage.table_fd_was_used = true;
            write_buf = std::make_unique<WriteBufferFromFileDescriptor>(storage.table_fd);
        }
        else
        {
            write_buf = std::make_unique<WriteBufferFromFile>(storage.path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
        }

        writer = FormatFactory().getOutput(storage.format_name, *write_buf, storage.getSampleBlock(), storage.context_global);
    }

    void write(const Block & block) override
    {
        writer->write(block);
    }

    void writePrefix() override
    {
        writer->writePrefix();
    }

    void writeSuffix() override
    {
        writer->writeSuffix();
    }

    void flush() override
    {
        writer->flush();
    }

private:
    StorageFile & storage;
    std::unique_lock<std::shared_mutex> lock;
    std::unique_ptr<WriteBufferFromFileDescriptor> write_buf;
    BlockOutputStreamPtr writer;
};

BlockOutputStreamPtr StorageFile::write(
    const ASTPtr & query,
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
        throw Exception("Can't rename table '" + table_name + "' binded to user-defined file (or FD)", ErrorCodes::DATABASE_ACCESS_DENIED);

    std::unique_lock<std::shared_mutex> lock(rwlock);

    std::string path_new = getTablePath(new_path_to_db, new_table_name, format_name);
    Poco::File(Poco::Path(path_new).parent()).createDirectories();
    Poco::File(path).renameTo(path_new);

    path = std::move(path_new);
}

}
