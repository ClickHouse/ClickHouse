#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

#include <fcntl.h>

#include <Poco/Path.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


static std::string getTablePath(const std::string & db_dir_path, const std::string & table_name, const std::string & format_name)
{
    return db_dir_path + escapeForFileName(table_name) + "/data." + escapeForFileName(format_name);
}

/// Both db_dir_path and table_path must be converted to absolute paths (in particular, path cannot contain '..').
static void checkCreationIsAllowed(Context & context_global, const std::string & db_dir_path, const std::string & table_path, int table_fd)
{
    if (context_global.getApplicationType() != Context::ApplicationType::SERVER)
        return;

    if (table_fd >= 0)
        throw Exception("Using file descriptor as source of storage isn't allowed for server daemons", ErrorCodes::DATABASE_ACCESS_DENIED);
    else if (!startsWith(table_path, db_dir_path))
        throw Exception("Part path " + table_path + " is not inside " + db_dir_path, ErrorCodes::DATABASE_ACCESS_DENIED);

    Poco::File table_path_poco_file = Poco::File(table_path);
    if (!table_path_poco_file.exists())
        throw Exception("File " + table_path + " is not exist", ErrorCodes::FILE_DOESNT_EXIST);
    else if (table_path_poco_file.isDirectory())
        throw Exception("File " + table_path + " must not be a directory", ErrorCodes::INCORRECT_FILE_NAME);
}


StorageFile::StorageFile(
        const std::string & table_path_,
        int table_fd_,
        const std::string & db_dir_path,
        const std::string & table_name_,
        const std::string & format_name_,
        const ColumnsDescription & columns_,
        Context & context_)
    : IStorage(columns_),
    table_name(table_name_), format_name(format_name_), context_global(context_), table_fd(table_fd_)
{
    if (table_fd < 0) /// Will use file
    {
        use_table_fd = false;

        if (!table_path_.empty()) /// Is user's file
        {
            Poco::Path poco_path = Poco::Path(table_path_);
            if (poco_path.isRelative())
                poco_path = Poco::Path(db_dir_path, poco_path);

            path = poco_path.absolute().toString();
            checkCreationIsAllowed(context_global, db_dir_path, path, table_fd);
            is_db_table = false;
        }
        else /// Is DB's file
        {
            if (db_dir_path.empty())
                throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);

            path = getTablePath(db_dir_path, table_name, format_name);
            is_db_table = true;
            Poco::File(Poco::Path(path).parent()).createDirectories();
        }
    }
    else /// Will use FD
    {
        checkCreationIsAllowed(context_global, db_dir_path, path, table_fd);

        is_db_table = false;
        use_table_fd = true;

        /// Save initial offset, it will be used for repeating SELECTs
        /// If FD isn't seekable (lseek returns -1), then the second and subsequent SELECTs will fail.
        table_fd_init_offset = lseek(table_fd, 0, SEEK_CUR);
    }
}


class StorageFileBlockInputStream : public IBlockInputStream
{
public:
    StorageFileBlockInputStream(StorageFile & storage_, const Context & context, UInt64 max_block_size)
        : storage(storage_)
    {
        if (storage.use_table_fd)
        {
            unique_lock = std::unique_lock(storage.rwlock);

            /// We could use common ReadBuffer and WriteBuffer in storage to leverage cache
            ///  and add ability to seek unseekable files, but cache sync isn't supported.

            if (storage.table_fd_was_used) /// We need seek to initial position
            {
                if (storage.table_fd_init_offset < 0)
                    throw Exception("File descriptor isn't seekable, inside " + storage.getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

                /// ReadBuffer's seek() doesn't make sense, since cache is empty
                if (lseek(storage.table_fd, storage.table_fd_init_offset, SEEK_SET) < 0)
                    throwFromErrno("Cannot seek file descriptor, inside " + storage.getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
            }

            storage.table_fd_was_used = true;
            read_buf = std::make_unique<ReadBufferFromFileDescriptor>(storage.table_fd);
        }
        else
        {
            shared_lock = std::shared_lock(storage.rwlock);

            read_buf = std::make_unique<ReadBufferFromFile>(storage.path);
        }

        reader = FormatFactory::instance().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    String getName() const override
    {
        return storage.getName();
    }

    Block readImpl() override
    {
        return reader->read();
    }

    Block getHeader() const override { return reader->getHeader(); }

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

    std::shared_lock<std::shared_mutex> shared_lock;
    std::unique_lock<std::shared_mutex> unique_lock;
};


BlockInputStreams StorageFile::read(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    BlockInputStreamPtr block_input = std::make_shared<StorageFileBlockInputStream>(*this, context, max_block_size);
    const ColumnsDescription & columns = getColumns();
    auto column_defaults = columns.getDefaults();
    if (column_defaults.empty())
        return {block_input};
    return {std::make_shared<AddingDefaultsBlockInputStream>(block_input, column_defaults, context)};
}


class StorageFileBlockOutputStream : public IBlockOutputStream
{
public:
    explicit StorageFileBlockOutputStream(StorageFile & storage_)
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

        writer = FormatFactory::instance().getOutput(storage.format_name, *write_buf, storage.getSampleBlock(), storage.context_global);
    }

    Block getHeader() const override { return storage.getSampleBlock(); }

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
    const ASTPtr & /*query*/,
    const Context & /*context*/)
{
    return std::make_shared<StorageFileBlockOutputStream>(*this);
}


void StorageFile::drop()
{
    /// Extra actions are not required.
}


void StorageFile::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    if (!is_db_table)
        throw Exception("Can't rename table '" + table_name + "' binded to user-defined file (or FD)", ErrorCodes::DATABASE_ACCESS_DENIED);

    std::unique_lock<std::shared_mutex> lock(rwlock);

    std::string path_new = getTablePath(new_path_to_db, new_table_name, format_name);
    Poco::File(Poco::Path(path_new).parent()).createDirectories();
    Poco::File(path).renameTo(path_new);

    path = std::move(path_new);
}


void registerStorageFile(StorageFactory & factory)
{
    factory.registerStorage("File", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (!(engine_args.size() == 1 || engine_args.size() == 2))
            throw Exception(
                "Storage File requires 1 or 2 arguments: name of used format and source.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        String format_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        int source_fd = -1;
        String source_path;
        if (engine_args.size() >= 2)
        {
            /// Will use FD if engine_args[1] is int literal or identifier with std* name

            if (auto opt_name = getIdentifierName(engine_args[1]))
            {
                if (*opt_name == "stdin")
                    source_fd = STDIN_FILENO;
                else if (*opt_name == "stdout")
                    source_fd = STDOUT_FILENO;
                else if (*opt_name == "stderr")
                    source_fd = STDERR_FILENO;
                else
                    throw Exception("Unknown identifier '" + *opt_name + "' in second arg of File storage constructor",
                                    ErrorCodes::UNKNOWN_IDENTIFIER);
            }
            else if (const auto * literal = engine_args[1]->as<ASTLiteral>())
            {
                auto type = literal->value.getType();
                if (type == Field::Types::Int64)
                    source_fd = static_cast<int>(literal->value.get<Int64>());
                else if (type == Field::Types::UInt64)
                    source_fd = static_cast<int>(literal->value.get<UInt64>());
                else if (type == Field::Types::String)
                    source_path = literal->value.get<String>();
            }
        }

        return StorageFile::create(
            source_path, source_fd,
            args.data_path,
            args.table_name, format_name, args.columns,
            args.context);
    });
}

}
