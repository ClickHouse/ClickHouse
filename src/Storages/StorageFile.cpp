#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/parseGlobs.h>
#include <Common/filesystemHelpers.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <fcntl.h>
#include <unistd.h>

#include <re2/re2.h>
#include <filesystem>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Pipe.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int TIMEOUT_EXCEEDED;
    extern const int INCOMPATIBLE_COLUMNS;
}

namespace
{

/* Recursive directory listing with matched paths as a result.
 * Have the same method in StorageHDFS.
 */
std::vector<std::string> listFilesWithRegexpMatching(const std::string & path_for_ls, const std::string & for_match, size_t & total_bytes_to_read)
{
    const size_t first_glob = for_match.find_first_of("*?{");

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
    const std::string suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'

    const size_t next_slash = suffix_with_globs.find('/', 1);
    auto regexp = makeRegexpPatternFromGlobs(suffix_with_globs.substr(0, next_slash));
    re2::RE2 matcher(regexp);

    std::vector<std::string> result;
    const std::string prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs);
    if (!fs::exists(prefix_without_globs))
        return result;

    const fs::directory_iterator end;
    for (fs::directory_iterator it(prefix_without_globs); it != end; ++it)
    {
        const std::string full_path = it->path().string();
        const size_t last_slash = full_path.rfind('/');
        const String file_name = full_path.substr(last_slash);
        const bool looking_for_directory = next_slash != std::string::npos;
        /// Condition is_directory means what kind of path is it in current iteration of ls
        if (!fs::is_directory(it->path()) && !looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                total_bytes_to_read += fs::file_size(it->path());
                result.push_back(it->path().string());
            }
        }
        else if (fs::is_directory(it->path()) && looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                Strings result_part = listFilesWithRegexpMatching(fs::path(full_path) / "", suffix_with_globs.substr(next_slash), total_bytes_to_read);
                std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
            }
        }
    }
    return result;
}

std::string getTablePath(const std::string & table_dir_path, const std::string & format_name)
{
    return table_dir_path + "/data." + escapeForFileName(format_name);
}

/// Both db_dir_path and table_path must be converted to absolute paths (in particular, path cannot contain '..').
void checkCreationIsAllowed(ContextPtr context_global, const std::string & db_dir_path, const std::string & table_path)
{
    if (context_global->getApplicationType() != Context::ApplicationType::SERVER)
        return;

    /// "/dev/null" is allowed for perf testing
    if (!fileOrSymlinkPathStartsWith(table_path, db_dir_path) && table_path != "/dev/null")
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File `{}` is not inside `{}`", table_path, db_dir_path);

    if (fs::exists(table_path) && fs::is_directory(table_path))
        throw Exception("File must not be a directory", ErrorCodes::INCORRECT_FILE_NAME);
}
}

Strings StorageFile::getPathsList(const String & table_path, const String & user_files_path, ContextPtr context, size_t & total_bytes_to_read)
{
    fs::path user_files_absolute_path = fs::weakly_canonical(user_files_path);
    fs::path fs_table_path(table_path);
    if (fs_table_path.is_relative())
        fs_table_path = user_files_absolute_path / fs_table_path;

    Strings paths;
    /// Do not use fs::canonical or fs::weakly_canonical.
    /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
    String path = fs::absolute(fs_table_path);
    path = fs::path(path).lexically_normal(); /// Normalize path.
    if (path.find_first_of("*?{") == std::string::npos)
    {
        std::error_code error;
        if (fs::exists(path))
            total_bytes_to_read += fs::file_size(path, error);
        paths.push_back(path);
    }
    else
        paths = listFilesWithRegexpMatching("/", path, total_bytes_to_read);

    for (const auto & cur_path : paths)
        checkCreationIsAllowed(context, user_files_absolute_path, cur_path);

    return paths;
}

bool StorageFile::isColumnOriented() const
{
    return format_name != "Distributed" && FormatFactory::instance().checkIfFormatIsColumnOriented(format_name);
}

StorageFile::StorageFile(int table_fd_, CommonArguments args)
    : StorageFile(args)
{
    if (args.getContext()->getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception("Using file descriptor as source of storage isn't allowed for server daemons", ErrorCodes::DATABASE_ACCESS_DENIED);
    if (args.format_name == "Distributed")
        throw Exception("Distributed format is allowed only with explicit file path", ErrorCodes::INCORRECT_FILE_NAME);

    is_db_table = false;
    use_table_fd = true;
    table_fd = table_fd_;

    /// Save initial offset, it will be used for repeating SELECTs
    /// If FD isn't seekable (lseek returns -1), then the second and subsequent SELECTs will fail.
    table_fd_init_offset = lseek(table_fd, 0, SEEK_CUR);
}

StorageFile::StorageFile(const std::string & table_path_, const std::string & user_files_path, CommonArguments args)
    : StorageFile(args)
{
    is_db_table = false;
    paths = getPathsList(table_path_, user_files_path, args.getContext(), total_bytes_to_read);

    if (args.format_name == "Distributed")
    {
        if (paths.empty())
            throw Exception("Cannot get table structure from file, because no files match specified name", ErrorCodes::INCORRECT_FILE_NAME);

        auto & first_path = paths[0];
        Block header = StorageDistributedDirectoryMonitor::createStreamFromFile(first_path)->getHeader();

        StorageInMemoryMetadata storage_metadata;
        auto columns = ColumnsDescription(header.getNamesAndTypesList());
        if (!args.columns.empty() && columns != args.columns)
            throw Exception("Table structure and file structure are different", ErrorCodes::INCOMPATIBLE_COLUMNS);
        storage_metadata.setColumns(columns);
        setInMemoryMetadata(storage_metadata);
    }
}

StorageFile::StorageFile(const std::string & relative_table_dir_path, CommonArguments args)
    : StorageFile(args)
{
    if (relative_table_dir_path.empty())
        throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);
    if (args.format_name == "Distributed")
        throw Exception("Distributed format is allowed only with explicit file path", ErrorCodes::INCORRECT_FILE_NAME);

    String table_dir_path = fs::path(base_path) / relative_table_dir_path / "";
    fs::create_directories(table_dir_path);
    paths = {getTablePath(table_dir_path, format_name)};
}

StorageFile::StorageFile(CommonArguments args)
    : IStorage(args.table_id)
    , format_name(args.format_name)
    , format_settings(args.format_settings)
    , compression_method(args.compression_method)
    , base_path(args.getContext()->getPath())
{
    StorageInMemoryMetadata storage_metadata;
    if (args.format_name != "Distributed")
        storage_metadata.setColumns(args.columns);

    storage_metadata.setConstraints(args.constraints);
    storage_metadata.setComment(args.comment);
    setInMemoryMetadata(storage_metadata);
}


static std::chrono::seconds getLockTimeout(ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    Int64 lock_timeout = settings.lock_acquire_timeout.totalSeconds();
    if (settings.max_execution_time.totalSeconds() != 0 && settings.max_execution_time.totalSeconds() < lock_timeout)
        lock_timeout = settings.max_execution_time.totalSeconds();
    return std::chrono::seconds{lock_timeout};
}

using StorageFilePtr = std::shared_ptr<StorageFile>;


class StorageFileSource : public SourceWithProgress
{
public:
    struct FilesInfo
    {
        std::vector<std::string> files;

        std::atomic<size_t> next_file_to_read = 0;

        bool need_path_column = false;
        bool need_file_column = false;
    };

    using FilesInfoPtr = std::shared_ptr<FilesInfo>;

    static Block getHeader(const StorageMetadataPtr & metadata_snapshot, bool need_path_column, bool need_file_column)
    {
        auto header = metadata_snapshot->getSampleBlock();

        /// Note: AddingDefaultsBlockInputStream doesn't change header.

        if (need_path_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_path"});
        if (need_file_column)
            header.insert({DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "_file"});

        return header;
    }

    static Block getBlockForSource(
        const StorageFilePtr & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const ColumnsDescription & columns_description,
        const FilesInfoPtr & files_info)
    {
        if (storage->isColumnOriented())
            return metadata_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical(), storage->getVirtuals(), storage->getStorageID());
        else
            return getHeader(metadata_snapshot, files_info->need_path_column, files_info->need_file_column);
    }

    StorageFileSource(
        std::shared_ptr<StorageFile> storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_,
        UInt64 max_block_size_,
        FilesInfoPtr files_info_,
        ColumnsDescription columns_description_)
        : SourceWithProgress(getBlockForSource(storage_, metadata_snapshot_, columns_description_, files_info_))
        , storage(std::move(storage_))
        , metadata_snapshot(metadata_snapshot_)
        , files_info(std::move(files_info_))
        , columns_description(std::move(columns_description_))
        , context(context_)
        , max_block_size(max_block_size_)
    {
        if (storage->use_table_fd)
        {
            unique_lock = std::unique_lock(storage->rwlock, getLockTimeout(context));
            if (!unique_lock)
                throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

            /// We could use common ReadBuffer and WriteBuffer in storage to leverage cache
            ///  and add ability to seek unseekable files, but cache sync isn't supported.

            if (storage->table_fd_was_used) /// We need seek to initial position
            {
                if (storage->table_fd_init_offset < 0)
                    throw Exception("File descriptor isn't seekable, inside " + storage->getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

                /// ReadBuffer's seek() doesn't make sense, since cache is empty
                if (lseek(storage->table_fd, storage->table_fd_init_offset, SEEK_SET) < 0)
                    throwFromErrno("Cannot seek file descriptor, inside " + storage->getName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
            }

            storage->table_fd_was_used = true;
        }
        else
        {
            shared_lock = std::shared_lock(storage->rwlock, getLockTimeout(context));
            if (!shared_lock)
                throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }

    String getName() const override
    {
        return storage->getName();
    }

    Chunk generate() override
    {
        while (!finished_generate)
        {
            /// Open file lazily on first read. This is needed to avoid too many open files from different streams.
            if (!reader)
            {
                if (!storage->use_table_fd)
                {
                    auto current_file = files_info->next_file_to_read.fetch_add(1);
                    if (current_file >= files_info->files.size())
                        return {};

                    current_path = files_info->files[current_file];

                    /// Special case for distributed format. Defaults are not needed here.
                    if (storage->format_name == "Distributed")
                    {
                        reader = StorageDistributedDirectoryMonitor::createStreamFromFile(current_path);
                        continue;
                    }
                }

                std::unique_ptr<ReadBuffer> nested_buffer;
                CompressionMethod method;

                if (storage->use_table_fd)
                {
                    nested_buffer = std::make_unique<ReadBufferFromFileDescriptor>(storage->table_fd);
                    method = chooseCompressionMethod("", storage->compression_method);
                }
                else
                {
                    nested_buffer = std::make_unique<ReadBufferFromFile>(current_path, context->getSettingsRef().max_read_buffer_size);
                    method = chooseCompressionMethod(current_path, storage->compression_method);
                }

                /// For clickhouse-local add progress callback to display progress bar.
                if (context->getApplicationType() == Context::ApplicationType::LOCAL)
                {
                    auto & in = static_cast<ReadBufferFromFileDescriptor &>(*nested_buffer);
                    in.setProgressCallback(context);
                }

                read_buf = wrapReadBufferWithCompressionMethod(std::move(nested_buffer), method);

                auto get_block_for_format = [&]() -> Block
                {
                    if (storage->isColumnOriented())
                        return metadata_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
                    return metadata_snapshot->getSampleBlock();
                };

                auto format = FormatFactory::instance().getInput(
                    storage->format_name, *read_buf, get_block_for_format(), context, max_block_size, storage->format_settings);

                reader = std::make_shared<InputStreamFromInputFormat>(format);

                if (columns_description.hasDefaults())
                    reader = std::make_shared<AddingDefaultsBlockInputStream>(reader, columns_description, context);

                reader->readPrefix();
            }

            if (auto res = reader->read())
            {
                Columns columns = res.getColumns();
                UInt64 num_rows = res.rows();

                /// Enrich with virtual columns.
                if (files_info->need_path_column)
                {
                    auto column = DataTypeString().createColumnConst(num_rows, current_path);
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                if (files_info->need_file_column)
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column = DataTypeString().createColumnConst(num_rows, std::move(file_name));
                    columns.push_back(column->convertToFullColumnIfConst());
                }

                return Chunk(std::move(columns), num_rows);
            }

            /// Read only once for file descriptor.
            if (storage->use_table_fd)
                finished_generate = true;

            /// Close file prematurely if stream was ended.
            reader->readSuffix();
            reader.reset();
            read_buf.reset();
        }

        return {};
    }


private:
    std::shared_ptr<StorageFile> storage;
    StorageMetadataPtr metadata_snapshot;
    FilesInfoPtr files_info;
    String current_path;
    Block sample_block;
    std::unique_ptr<ReadBuffer> read_buf;
    BlockInputStreamPtr reader;

    ColumnsDescription columns_description;

    ContextPtr context;    /// TODO Untangle potential issues with context lifetime.
    UInt64 max_block_size;

    bool finished_generate = false;

    std::shared_lock<std::shared_timed_mutex> shared_lock;
    std::unique_lock<std::shared_timed_mutex> unique_lock;
};

Pipe StorageFile::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    BlockInputStreams blocks_input;

    if (use_table_fd)   /// need to call ctr BlockInputStream
        paths = {""};   /// when use fd, paths are empty
    else
        if (paths.size() == 1 && !fs::exists(paths[0]))
        {
            if (context->getSettingsRef().engine_file_empty_if_not_exists)
                return Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));
            else
                throw Exception("File " + paths[0] + " doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);
        }


    auto files_info = std::make_shared<StorageFileSource::FilesInfo>();
    files_info->files = paths;

    for (const auto & column : column_names)
    {
        if (column == "_path")
            files_info->need_path_column = true;
        if (column == "_file")
            files_info->need_file_column = true;
    }

    auto this_ptr = std::static_pointer_cast<StorageFile>(shared_from_this());

    if (num_streams > paths.size())
        num_streams = paths.size();

    Pipes pipes;
    pipes.reserve(num_streams);

    /// Set total number of bytes to process. For progress bar.
    auto progress_callback = context->getFileProgressCallback();
    if (context->getApplicationType() == Context::ApplicationType::LOCAL && progress_callback)
        progress_callback(FileProgress(0, total_bytes_to_read));

    for (size_t i = 0; i < num_streams; ++i)
    {
        const auto get_columns_for_format = [&]() -> ColumnsDescription
        {
            if (isColumnOriented())
                return ColumnsDescription{
                    metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID()).getNamesAndTypesList()};
            else
                return metadata_snapshot->getColumns();
        };
        pipes.emplace_back(std::make_shared<StorageFileSource>(
            this_ptr, metadata_snapshot, context, max_block_size, files_info, get_columns_for_format()));
    }

    return Pipe::unitePipes(std::move(pipes));
}


class StorageFileBlockOutputStream : public IBlockOutputStream
{
public:
    explicit StorageFileBlockOutputStream(
        StorageFile & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        std::unique_lock<std::shared_timed_mutex> && lock_,
        const CompressionMethod compression_method,
        ContextPtr context,
        const std::optional<FormatSettings> & format_settings,
        int & flags)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , lock(std::move(lock_))
    {
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

        std::unique_ptr<WriteBufferFromFileDescriptor> naked_buffer = nullptr;
        if (storage.use_table_fd)
        {
            /** NOTE: Using real file bounded to FD may be misleading:
              * SELECT *; INSERT insert_data; SELECT *; last SELECT returns initil_fd_data + insert_data
              * INSERT data; SELECT *; last SELECT returns only insert_data
              */
            storage.table_fd_was_used = true;
            naked_buffer = std::make_unique<WriteBufferFromFileDescriptor>(storage.table_fd, DBMS_DEFAULT_BUFFER_SIZE);
        }
        else
        {
            if (storage.paths.size() != 1)
                throw Exception("Table '" + storage.getStorageID().getNameForLogs() + "' is in readonly mode because of globs in filepath", ErrorCodes::DATABASE_ACCESS_DENIED);
            flags |= O_WRONLY | O_APPEND | O_CREAT;
            naked_buffer = std::make_unique<WriteBufferFromFile>(storage.paths[0], DBMS_DEFAULT_BUFFER_SIZE, flags);
        }

        /// In case of CSVWithNames we have already written prefix.
        if (naked_buffer->size())
            prefix_written = true;

        write_buf = wrapWriteBufferWithCompressionMethod(std::move(naked_buffer), compression_method, 3);

        writer = FormatFactory::instance().getOutputStreamParallelIfPossible(storage.format_name,
            *write_buf, metadata_snapshot->getSampleBlock(), context,
            {}, format_settings);
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        writer->write(block);
    }

    void writePrefix() override
    {
        if (!prefix_written)
            writer->writePrefix();
        prefix_written = true;
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
    StorageMetadataPtr metadata_snapshot;
    std::unique_lock<std::shared_timed_mutex> lock;
    std::unique_ptr<WriteBuffer> write_buf;
    BlockOutputStreamPtr writer;
    bool prefix_written{false};
};

BlockOutputStreamPtr StorageFile::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context)
{
    if (format_name == "Distributed")
        throw Exception("Method write is not implemented for Distributed format", ErrorCodes::NOT_IMPLEMENTED);

    int flags = 0;

    std::string path;
    if (context->getSettingsRef().engine_file_truncate_on_insert)
        flags |= O_TRUNC;

    if (!paths.empty())
    {
        path = paths[0];
        fs::create_directories(fs::path(path).parent_path());
    }

    return std::make_shared<StorageFileBlockOutputStream>(
        *this,
        metadata_snapshot,
        std::unique_lock{rwlock, getLockTimeout(context)},
        chooseCompressionMethod(path, compression_method),
        context,
        format_settings,
        flags);
}

bool StorageFile::storesDataOnDisk() const
{
    return is_db_table;
}

Strings StorageFile::getDataPaths() const
{
    if (paths.empty())
        throw Exception("Table '" + getStorageID().getNameForLogs() + "' is in readonly mode", ErrorCodes::DATABASE_ACCESS_DENIED);
    return paths;
}

void StorageFile::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    if (!is_db_table)
        throw Exception("Can't rename table " + getStorageID().getNameForLogs() + " bounded to user-defined file (or FD)", ErrorCodes::DATABASE_ACCESS_DENIED);

    if (paths.size() != 1)
        throw Exception("Can't rename table " + getStorageID().getNameForLogs() + " in readonly mode", ErrorCodes::DATABASE_ACCESS_DENIED);

    std::string path_new = getTablePath(base_path + new_path_to_table_data, format_name);
    if (path_new == paths[0])
        return;

    fs::create_directories(fs::path(path_new).parent_path());
    fs::rename(paths[0], path_new);

    paths[0] = std::move(path_new);
    renameInMemory(new_table_id);
}

void StorageFile::truncate(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /* metadata_snapshot */,
    ContextPtr /* context */,
    TableExclusiveLockHolder &)
{
    if (paths.size() != 1)
        throw Exception("Can't truncate table '" + getStorageID().getNameForLogs() + "' in readonly mode", ErrorCodes::DATABASE_ACCESS_DENIED);

    if (use_table_fd)
    {
        if (0 != ::ftruncate(table_fd, 0))
            throwFromErrno("Cannot truncate file at fd " + toString(table_fd), ErrorCodes::CANNOT_TRUNCATE_FILE);
    }
    else
    {
        if (!fs::exists(paths[0]))
            return;

        if (0 != ::truncate(paths[0].c_str(), 0))
            throwFromErrnoWithPath("Cannot truncate file " + paths[0], paths[0], ErrorCodes::CANNOT_TRUNCATE_FILE);
    }
}


void registerStorageFile(StorageFactory & factory)
{
    StorageFactory::StorageFeatures storage_features{
        .supports_settings = true,
        .source_access_type = AccessType::FILE
    };

    factory.registerStorage(
        "File",
        [](const StorageFactory::Arguments & factory_args)
        {
            StorageFile::CommonArguments storage_args
            {
                WithContext(factory_args.getContext()),
                factory_args.table_id,
                {},
                {},
                {},
                factory_args.columns,
                factory_args.constraints,
                factory_args.comment,
            };

            ASTs & engine_args_ast = factory_args.engine_args;

            if (!(engine_args_ast.size() >= 1 && engine_args_ast.size() <= 3)) // NOLINT
                throw Exception(
                    "Storage File requires from 1 to 3 arguments: name of used format, source and compression_method.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            engine_args_ast[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args_ast[0], factory_args.getLocalContext());
            storage_args.format_name = engine_args_ast[0]->as<ASTLiteral &>().value.safeGet<String>();

            // Use format settings from global server context + settings from
            // the SETTINGS clause of the create query. Settings from current
            // session and user are ignored.
            if (factory_args.storage_def->settings)
            {
                FormatFactorySettings user_format_settings;

                // Apply changed settings from global context, but ignore the
                // unknown ones, because we only have the format settings here.
                const auto & changes = factory_args.getContext()->getSettingsRef().changes();
                for (const auto & change : changes)
                {
                    if (user_format_settings.has(change.name))
                    {
                        user_format_settings.set(change.name, change.value);
                    }
                }

                // Apply changes from SETTINGS clause, with validation.
                user_format_settings.applyChanges(
                    factory_args.storage_def->settings->changes);

                storage_args.format_settings = getFormatSettings(
                    factory_args.getContext(), user_format_settings);
            }
            else
            {
                storage_args.format_settings = getFormatSettings(
                    factory_args.getContext());
            }

            if (engine_args_ast.size() == 1) /// Table in database
                return StorageFile::create(factory_args.relative_data_path, storage_args);

            /// Will use FD if engine_args[1] is int literal or identifier with std* name
            int source_fd = -1;
            String source_path;

            if (auto opt_name = tryGetIdentifierName(engine_args_ast[1]))
            {
                if (*opt_name == "stdin")
                    source_fd = STDIN_FILENO;
                else if (*opt_name == "stdout")
                    source_fd = STDOUT_FILENO;
                else if (*opt_name == "stderr")
                    source_fd = STDERR_FILENO;
                else
                    throw Exception(
                        "Unknown identifier '" + *opt_name + "' in second arg of File storage constructor", ErrorCodes::UNKNOWN_IDENTIFIER);
            }
            else if (const auto * literal = engine_args_ast[1]->as<ASTLiteral>())
            {
                auto type = literal->value.getType();
                if (type == Field::Types::Int64)
                    source_fd = static_cast<int>(literal->value.get<Int64>());
                else if (type == Field::Types::UInt64)
                    source_fd = static_cast<int>(literal->value.get<UInt64>());
                else if (type == Field::Types::String)
                    source_path = literal->value.get<String>();
                else
                    throw Exception("Second argument must be path or file descriptor", ErrorCodes::BAD_ARGUMENTS);
            }

            if (engine_args_ast.size() == 3)
            {
                engine_args_ast[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args_ast[2], factory_args.getLocalContext());
                storage_args.compression_method = engine_args_ast[2]->as<ASTLiteral &>().value.safeGet<String>();
            }
            else
                storage_args.compression_method = "auto";

            if (0 <= source_fd) /// File descriptor
                return StorageFile::create(source_fd, storage_args);
            else /// User's file
                return StorageFile::create(source_path, factory_args.getContext()->getUserFilesPath(), storage_args);
        },
        storage_features);
}


NamesAndTypesList StorageFile::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}
}
