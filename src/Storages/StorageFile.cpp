#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/PartitionedSink.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/ISource.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/parseGlobs.h>
#include <Common/filesystemHelpers.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <re2/re2.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int TIMEOUT_EXCEEDED;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int CANNOT_STAT;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_APPEND_TO_FILE;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

namespace
{

/* Recursive directory listing with matched paths as a result.
 * Have the same method in StorageHDFS.
 */
void listFilesWithRegexpMatchingImpl(
    const std::string & path_for_ls,
    const std::string & for_match,
    size_t & total_bytes_to_read,
    std::vector<std::string> & result)
{
    const size_t first_glob = for_match.find_first_of("*?{");

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
    const std::string suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'

    const size_t next_slash = suffix_with_globs.find('/', 1);
    auto regexp = makeRegexpPatternFromGlobs(suffix_with_globs.substr(0, next_slash));
    re2::RE2 matcher(regexp);

    const std::string prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs);
    if (!fs::exists(prefix_without_globs))
        return;

    const fs::directory_iterator end;
    for (fs::directory_iterator it(prefix_without_globs); it != end; ++it)
    {
        const std::string full_path = it->path().string();
        const size_t last_slash = full_path.rfind('/');
        const String file_name = full_path.substr(last_slash);
        const bool looking_for_directory = next_slash != std::string::npos;

        /// Condition is_directory means what kind of path is it in current iteration of ls
        if (!it->is_directory() && !looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                total_bytes_to_read += it->file_size();
                result.push_back(it->path().string());
            }
        }
        else if (it->is_directory() && looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                listFilesWithRegexpMatchingImpl(fs::path(full_path) / "", suffix_with_globs.substr(next_slash), total_bytes_to_read, result);
            }
        }
    }
}

std::vector<std::string> listFilesWithRegexpMatching(
    const std::string & path_for_ls,
    const std::string & for_match,
    size_t & total_bytes_to_read)
{
    std::vector<std::string> result;
    listFilesWithRegexpMatchingImpl(path_for_ls, for_match, total_bytes_to_read, result);
    return result;
}

std::string getTablePath(const std::string & table_dir_path, const std::string & format_name)
{
    return table_dir_path + "/data." + escapeForFileName(format_name);
}

/// Both db_dir_path and table_path must be converted to absolute paths (in particular, path cannot contain '..').
void checkCreationIsAllowed(
    ContextPtr context_global,
    const std::string & db_dir_path,
    const std::string & table_path,
    bool can_be_directory)
{
    if (context_global->getApplicationType() != Context::ApplicationType::SERVER)
        return;

    /// "/dev/null" is allowed for perf testing
    if (!fileOrSymlinkPathStartsWith(table_path, db_dir_path) && table_path != "/dev/null")
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "File `{}` is not inside `{}`", table_path, db_dir_path);

    if (can_be_directory)
    {
        auto table_path_stat = fs::status(table_path);
        if (fs::exists(table_path_stat) && fs::is_directory(table_path_stat))
            throw Exception("File must not be a directory", ErrorCodes::INCORRECT_FILE_NAME);
    }
}

std::unique_ptr<ReadBuffer> createReadBuffer(
    const String & current_path,
    bool use_table_fd,
    const String & storage_name,
    int table_fd,
    const String & compression_method,
    ContextPtr context)
{
    std::unique_ptr<ReadBuffer> nested_buffer;
    CompressionMethod method;

    struct stat file_stat{};

    if (use_table_fd)
    {
        /// Check if file descriptor allows random reads (and reading it twice).
        if (0 != fstat(table_fd, &file_stat))
            throwFromErrno("Cannot stat table file descriptor, inside " + storage_name, ErrorCodes::CANNOT_STAT);

        if (S_ISREG(file_stat.st_mode))
            nested_buffer = std::make_unique<ReadBufferFromFileDescriptorPRead>(table_fd);
        else
            nested_buffer = std::make_unique<ReadBufferFromFileDescriptor>(table_fd);

        method = chooseCompressionMethod("", compression_method);
    }
    else
    {
        /// Check if file descriptor allows random reads (and reading it twice).
        if (0 != stat(current_path.c_str(), &file_stat))
            throwFromErrno("Cannot stat file " + current_path, ErrorCodes::CANNOT_STAT);

        if (S_ISREG(file_stat.st_mode))
            nested_buffer = std::make_unique<ReadBufferFromFilePRead>(current_path, context->getSettingsRef().max_read_buffer_size);
        else
            nested_buffer = std::make_unique<ReadBufferFromFile>(current_path, context->getSettingsRef().max_read_buffer_size);

        method = chooseCompressionMethod(current_path, compression_method);
    }

    /// For clickhouse-local and clickhouse-client add progress callback to display progress bar.
    if (context->getApplicationType() == Context::ApplicationType::LOCAL
        || context->getApplicationType() == Context::ApplicationType::CLIENT)
    {
        auto & in = static_cast<ReadBufferFromFileDescriptor &>(*nested_buffer);
        in.setProgressCallback(context);
    }

    auto zstd_window_log_max = context->getSettingsRef().zstd_window_log_max;
    return wrapReadBufferWithCompressionMethod(std::move(nested_buffer), method, zstd_window_log_max);
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
    String path = fs::absolute(fs_table_path).lexically_normal(); /// Normalize path.
    bool can_be_directory = true;

    if (path.find(PartitionedSink::PARTITION_ID_WILDCARD) != std::string::npos)
    {
        paths.push_back(path);
    }
    else if (path.find_first_of("*?{") == std::string::npos)
    {
        std::error_code error;
        size_t size = fs::file_size(path, error);
        if (!error)
            total_bytes_to_read += size;

        paths.push_back(path);
    }
    else
    {
        /// We list only non-directory files.
        paths = listFilesWithRegexpMatching("/", path, total_bytes_to_read);
        can_be_directory = false;
    }

    for (const auto & cur_path : paths)
        checkCreationIsAllowed(context, user_files_absolute_path, cur_path, can_be_directory);

    return paths;
}

ColumnsDescription StorageFile::getTableStructureFromFileDescriptor(ContextPtr context)
{
    /// If we want to read schema from file descriptor we should create
    /// a read buffer from fd, create a checkpoint, read some data required
    /// for schema inference, rollback to checkpoint and then use the created
    /// peekable read buffer on the first read from storage. It's needed because
    /// in case of file descriptor we have a stream of data and we cannot
    /// start reading data from the beginning after reading some data for
    /// schema inference.
    ReadBufferIterator read_buffer_iterator = [&]()
    {
        /// We will use PeekableReadBuffer to create a checkpoint, so we need a place
        /// where we can store the original read buffer.
        read_buffer_from_fd = createReadBuffer("", true, getName(), table_fd, compression_method, context);
        auto read_buf = std::make_unique<PeekableReadBuffer>(*read_buffer_from_fd);
        read_buf->setCheckpoint();
        return read_buf;
    };

    auto columns = readSchemaFromFormat(format_name, format_settings, read_buffer_iterator, false, context, peekable_read_buffer_from_fd);
    if (peekable_read_buffer_from_fd)
    {
        /// If we have created read buffer in readSchemaFromFormat we should rollback to checkpoint.
        assert_cast<PeekableReadBuffer *>(peekable_read_buffer_from_fd.get())->rollbackToCheckpoint();
        has_peekable_read_buffer_from_fd = true;
    }
    return columns;
}

ColumnsDescription StorageFile::getTableStructureFromFile(
    const String & format,
    const std::vector<String> & paths,
    const String & compression_method,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context)
{
    if (format == "Distributed")
    {
        if (paths.empty())
            throw Exception(
                "Cannot get table structure from file, because no files match specified name", ErrorCodes::INCORRECT_FILE_NAME);

        auto source = StorageDistributedDirectoryMonitor::createSourceFromFile(paths[0]);
        return ColumnsDescription(source->getOutputs().front().getHeader().getNamesAndTypesList());
    }

    if (paths.empty() && !FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format))
        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "Cannot extract table structure from {} format file, because there are no files with provided path. You must specify "
            "table structure manually",
            format);

    ReadBufferIterator read_buffer_iterator = [&, it = paths.begin()]() mutable -> std::unique_ptr<ReadBuffer>
    {
        if (it == paths.end())
            return nullptr;

        return createReadBuffer(*it++, false, "File", -1, compression_method, context);
    };

    return readSchemaFromFormat(format, format_settings, read_buffer_iterator, paths.size() > 1, context);
}

bool StorageFile::supportsSubsetOfColumns() const
{
    return format_name != "Distributed" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name);
}

StorageFile::StorageFile(int table_fd_, CommonArguments args)
    : StorageFile(args)
{
    struct stat buf;
    int res = fstat(table_fd_, &buf);
    if (-1 == res)
        throwFromErrno("Cannot execute fstat", res, ErrorCodes::CANNOT_FSTAT);
    total_bytes_to_read = buf.st_size;

    if (args.getContext()->getApplicationType() == Context::ApplicationType::SERVER)
        throw Exception("Using file descriptor as source of storage isn't allowed for server daemons", ErrorCodes::DATABASE_ACCESS_DENIED);
    if (args.format_name == "Distributed")
        throw Exception("Distributed format is allowed only with explicit file path", ErrorCodes::INCORRECT_FILE_NAME);

    is_db_table = false;
    use_table_fd = true;
    table_fd = table_fd_;
    setStorageMetadata(args);
}

StorageFile::StorageFile(const std::string & table_path_, const std::string & user_files_path, CommonArguments args)
    : StorageFile(args)
{
    is_db_table = false;
    paths = getPathsList(table_path_, user_files_path, args.getContext(), total_bytes_to_read);
    is_path_with_globs = paths.size() > 1;
    if (!paths.empty())
        path_for_partitioned_write = paths.front();
    else
        path_for_partitioned_write = table_path_;

    setStorageMetadata(args);
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

    std::error_code error;
    size_t size = fs::file_size(paths[0], error);
    if (!error)
        total_bytes_to_read = size;

    setStorageMetadata(args);
}

StorageFile::StorageFile(CommonArguments args)
    : IStorage(args.table_id)
    , format_name(args.format_name)
    , format_settings(args.format_settings)
    , compression_method(args.compression_method)
    , base_path(args.getContext()->getPath())
{
    if (format_name != "Distributed")
        FormatFactory::instance().checkFormatName(format_name);
}

void StorageFile::setStorageMetadata(CommonArguments args)
{
    StorageInMemoryMetadata storage_metadata;

    if (args.format_name == "Distributed" || args.columns.empty())
    {
        ColumnsDescription columns;
        if (use_table_fd)
            columns = getTableStructureFromFileDescriptor(args.getContext());
        else
        {
            columns = getTableStructureFromFile(format_name, paths, compression_method, format_settings, args.getContext());
            if (!args.columns.empty() && args.columns != columns)
                throw Exception("Table structure and file structure are different", ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
        storage_metadata.setColumns(columns);
    }
    else
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


class StorageFileSource : public ISource
{
public:
    struct FilesInfo
    {
        std::vector<std::string> files;

        std::atomic<size_t> next_file_to_read = 0;

        bool need_path_column = false;
        bool need_file_column = false;

        size_t total_bytes_to_read = 0;
    };

    using FilesInfoPtr = std::shared_ptr<FilesInfo>;

    static Block getHeader(const StorageMetadataPtr & metadata_snapshot, bool need_path_column, bool need_file_column)
    {
        auto header = metadata_snapshot->getSampleBlock();

        /// Note: AddingDefaultsTransform doesn't change header.

        if (need_path_column)
            header.insert(
                {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
                 std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                 "_path"});
        if (need_file_column)
            header.insert(
                {DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumn(),
                 std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                 "_file"});

        return header;
    }

    static Block getBlockForSource(
        const StorageFilePtr & storage,
        const StorageSnapshotPtr & storage_snapshot,
        const ColumnsDescription & columns_description,
        const FilesInfoPtr & files_info)
    {
        if (storage->supportsSubsetOfColumns())
            return storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
        else
            return getHeader(storage_snapshot->metadata, files_info->need_path_column, files_info->need_file_column);
    }

    StorageFileSource(
        std::shared_ptr<StorageFile> storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        UInt64 max_block_size_,
        FilesInfoPtr files_info_,
        ColumnsDescription columns_description_,
        std::unique_ptr<ReadBuffer> read_buf_)
        : ISource(getBlockForSource(storage_, storage_snapshot_, columns_description_, files_info_))
        , storage(std::move(storage_))
        , storage_snapshot(storage_snapshot_)
        , files_info(std::move(files_info_))
        , read_buf(std::move(read_buf_))
        , columns_description(std::move(columns_description_))
        , context(context_)
        , max_block_size(max_block_size_)
    {
        if (!storage->use_table_fd)
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
                        pipeline = std::make_unique<QueryPipeline>(StorageDistributedDirectoryMonitor::createSourceFromFile(current_path));
                        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
                        continue;
                    }
                }

                if (!read_buf)
                    read_buf = createReadBuffer(current_path, storage->use_table_fd, storage->getName(), storage->table_fd, storage->compression_method, context);

                auto get_block_for_format = [&]() -> Block
                {
                    if (storage->supportsSubsetOfColumns())
                        return storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
                    return storage_snapshot->metadata->getSampleBlock();
                };

                auto format = context->getInputFormat(
                    storage->format_name, *read_buf, get_block_for_format(), max_block_size, storage->format_settings);

                QueryPipelineBuilder builder;
                builder.init(Pipe(format));

                if (columns_description.hasDefaults())
                {
                    builder.addSimpleTransform([&](const Block & header)
                    {
                        return std::make_shared<AddingDefaultsTransform>(header, columns_description, *format, context);
                    });
                }

                pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));

                reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
            }

            Chunk chunk;
            if (reader->pull(chunk))
            {
                UInt64 num_rows = chunk.getNumRows();

                /// Enrich with virtual columns.
                if (files_info->need_path_column)
                {
                    auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, current_path);
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }

                if (files_info->need_file_column)
                {
                    size_t last_slash_pos = current_path.find_last_of('/');
                    auto file_name = current_path.substr(last_slash_pos + 1);

                    auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(num_rows, std::move(file_name));
                    chunk.addColumn(column->convertToFullColumnIfConst());
                }

                if (num_rows)
                {
                    auto bytes_per_row = std::ceil(static_cast<double>(chunk.bytes()) / num_rows);
                    size_t total_rows_approx = std::ceil(static_cast<double>(files_info->total_bytes_to_read) / bytes_per_row);
                    total_rows_approx_accumulated += total_rows_approx;
                    ++total_rows_count_times;
                    total_rows_approx = total_rows_approx_accumulated / total_rows_count_times;

                    /// We need to add diff, because total_rows_approx is incremental value.
                    /// It would be more correct to send total_rows_approx as is (not a diff),
                    /// but incrementation of total_rows_to_read does not allow that.
                    /// A new field can be introduces for that to be sent to client, but it does not worth it.
                    if (total_rows_approx > total_rows_approx_prev)
                    {
                        size_t diff = total_rows_approx - total_rows_approx_prev;
                        addTotalRowsApprox(diff);
                        total_rows_approx_prev = total_rows_approx;
                    }
                }
                return chunk;
            }

            /// Read only once for file descriptor.
            if (storage->use_table_fd)
                finished_generate = true;

            /// Close file prematurely if stream was ended.
            reader.reset();
            pipeline.reset();
            read_buf.reset();
        }

        return {};
    }


private:
    std::shared_ptr<StorageFile> storage;
    StorageSnapshotPtr storage_snapshot;
    FilesInfoPtr files_info;
    String current_path;
    Block sample_block;
    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;

    ColumnsDescription columns_description;

    ContextPtr context;    /// TODO Untangle potential issues with context lifetime.
    UInt64 max_block_size;

    bool finished_generate = false;

    std::shared_lock<std::shared_timed_mutex> shared_lock;

    UInt64 total_rows_approx_accumulated = 0;
    size_t total_rows_count_times = 0;
    UInt64 total_rows_approx_prev = 0;
};


Pipe StorageFile::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    if (use_table_fd)
    {
        paths = {""};   /// when use fd, paths are empty
    }
    else
    {
        if (paths.size() == 1 && !fs::exists(paths[0]))
        {
            if (context->getSettingsRef().engine_file_empty_if_not_exists)
                return Pipe(std::make_shared<NullSource>(storage_snapshot->getSampleBlockForColumns(column_names)));
            else
                throw Exception("File " + paths[0] + " doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);
        }
    }

    auto files_info = std::make_shared<StorageFileSource::FilesInfo>();
    files_info->files = paths;
    files_info->total_bytes_to_read = total_bytes_to_read;

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

    if (progress_callback)
        progress_callback(FileProgress(0, total_bytes_to_read));

    for (size_t i = 0; i < num_streams; ++i)
    {
        const auto get_columns_for_format = [&]() -> ColumnsDescription
        {
            if (supportsSubsetOfColumns())
                return storage_snapshot->getDescriptionForColumns(column_names);
            else
                return storage_snapshot->metadata->getColumns();
        };

        /// In case of reading from fd we have to check whether we have already created
        /// the read buffer from it in Storage constructor (for schema inference) or not.
        /// If yes, then we should use it in StorageFileSource. Atomic bool flag is needed
        /// to prevent data race in case of parallel reads.
        std::unique_ptr<ReadBuffer> read_buffer;
        if (has_peekable_read_buffer_from_fd.exchange(false))
            read_buffer = std::move(peekable_read_buffer_from_fd);

        pipes.emplace_back(std::make_shared<StorageFileSource>(
            this_ptr, storage_snapshot, context, max_block_size, files_info, get_columns_for_format(), std::move(read_buffer)));
    }

    return Pipe::unitePipes(std::move(pipes));
}


class StorageFileSink final : public SinkToStorage
{
public:
    StorageFileSink(
        const StorageMetadataPtr & metadata_snapshot_,
        const String & table_name_for_log_,
        int table_fd_,
        bool use_table_fd_,
        std::string base_path_,
        std::string path_,
        const CompressionMethod compression_method_,
        const std::optional<FormatSettings> & format_settings_,
        const String format_name_,
        ContextPtr context_,
        int flags_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , metadata_snapshot(metadata_snapshot_)
        , table_name_for_log(table_name_for_log_)
        , table_fd(table_fd_)
        , use_table_fd(use_table_fd_)
        , base_path(base_path_)
        , path(path_)
        , compression_method(compression_method_)
        , format_name(format_name_)
        , format_settings(format_settings_)
        , context(context_)
        , flags(flags_)
    {
        initialize();
    }

    StorageFileSink(
        const StorageMetadataPtr & metadata_snapshot_,
        const String & table_name_for_log_,
        std::unique_lock<std::shared_timed_mutex> && lock_,
        int table_fd_,
        bool use_table_fd_,
        std::string base_path_,
        const std::string & path_,
        const CompressionMethod compression_method_,
        const std::optional<FormatSettings> & format_settings_,
        const String format_name_,
        ContextPtr context_,
        int flags_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , metadata_snapshot(metadata_snapshot_)
        , table_name_for_log(table_name_for_log_)
        , table_fd(table_fd_)
        , use_table_fd(use_table_fd_)
        , base_path(base_path_)
        , path(path_)
        , compression_method(compression_method_)
        , format_name(format_name_)
        , format_settings(format_settings_)
        , context(context_)
        , flags(flags_)
        , lock(std::move(lock_))
    {
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);
        initialize();
    }

    void initialize()
    {
        std::unique_ptr<WriteBufferFromFileDescriptor> naked_buffer = nullptr;
        if (use_table_fd)
        {
            naked_buffer = std::make_unique<WriteBufferFromFileDescriptor>(table_fd, DBMS_DEFAULT_BUFFER_SIZE);
        }
        else
        {
            flags |= O_WRONLY | O_APPEND | O_CREAT;
            naked_buffer = std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, flags);
        }

        /// In case of formats with prefixes if file is not empty we have already written prefix.
        bool do_not_write_prefix = naked_buffer->size();

        write_buf = wrapWriteBufferWithCompressionMethod(std::move(naked_buffer), compression_method, 3);

        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format_name,
            *write_buf, metadata_snapshot->getSampleBlock(), context,
            {}, format_settings);

        if (do_not_write_prefix)
            writer->doNotWritePrefix();
    }

    String getName() const override { return "StorageFileSink"; }

    void consume(Chunk chunk) override
    {
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onException() override
    {
        if (!writer)
            return;
        onFinish();
    }

    void onFinish() override
    {
        try
        {
            writer->finalize();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            writer.reset();
            throw;
        }
    }

private:
    StorageMetadataPtr metadata_snapshot;
    String table_name_for_log;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;

    int table_fd;
    bool use_table_fd;
    std::string base_path;
    std::string path;
    CompressionMethod compression_method;
    std::string format_name;
    std::optional<FormatSettings> format_settings;

    ContextPtr context;
    int flags;
    std::unique_lock<std::shared_timed_mutex> lock;
};

class PartitionedStorageFileSink : public PartitionedSink
{
public:
    PartitionedStorageFileSink(
        const ASTPtr & partition_by,
        const StorageMetadataPtr & metadata_snapshot_,
        const String & table_name_for_log_,
        std::unique_lock<std::shared_timed_mutex> && lock_,
        String base_path_,
        String path_,
        const CompressionMethod compression_method_,
        const std::optional<FormatSettings> & format_settings_,
        const String format_name_,
        ContextPtr context_,
        int flags_)
        : PartitionedSink(partition_by, context_, metadata_snapshot_->getSampleBlock())
        , path(path_)
        , metadata_snapshot(metadata_snapshot_)
        , table_name_for_log(table_name_for_log_)
        , base_path(base_path_)
        , compression_method(compression_method_)
        , format_name(format_name_)
        , format_settings(format_settings_)
        , context(context_)
        , flags(flags_)
        , lock(std::move(lock_))
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_path = PartitionedSink::replaceWildcards(path, partition_id);
        PartitionedSink::validatePartitionKey(partition_path, true);
        checkCreationIsAllowed(context, context->getUserFilesPath(), partition_path, /*can_be_directory=*/ true);
        return std::make_shared<StorageFileSink>(
            metadata_snapshot,
            table_name_for_log,
            -1,
            /* use_table_fd */false,
            base_path,
            partition_path,
            compression_method,
            format_settings,
            format_name,
            context,
            flags);
    }

private:
    const String path;
    StorageMetadataPtr metadata_snapshot;
    String table_name_for_log;

    std::string base_path;
    CompressionMethod compression_method;
    std::string format_name;
    std::optional<FormatSettings> format_settings;

    ContextPtr context;
    int flags;
    std::unique_lock<std::shared_timed_mutex> lock;
};


SinkToStoragePtr StorageFile::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context)
{
    if (format_name == "Distributed")
        throw Exception("Method write is not implemented for Distributed format", ErrorCodes::NOT_IMPLEMENTED);

    int flags = 0;

    if (context->getSettingsRef().engine_file_truncate_on_insert)
        flags |= O_TRUNC;

    bool has_wildcards = path_for_partitioned_write.find(PartitionedSink::PARTITION_ID_WILDCARD) != String::npos;
    const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
    bool is_partitioned_implementation = insert_query && insert_query->partition_by && has_wildcards;

    if (is_partitioned_implementation)
    {
        if (path_for_partitioned_write.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty path for partitioned write");

        fs::create_directories(fs::path(path_for_partitioned_write).parent_path());

        return std::make_shared<PartitionedStorageFileSink>(
            insert_query->partition_by,
            metadata_snapshot,
            getStorageID().getNameForLogs(),
            std::unique_lock{rwlock, getLockTimeout(context)},
            base_path,
            path_for_partitioned_write,
            chooseCompressionMethod(path_for_partitioned_write, compression_method),
            format_settings,
            format_name,
            context,
            flags);
    }
    else
    {
        String path;
        if (!paths.empty())
        {
            if (is_path_with_globs)
                throw Exception("Table '" + getStorageID().getNameForLogs() + "' is in readonly mode because of globs in filepath", ErrorCodes::DATABASE_ACCESS_DENIED);

            path = paths.back();
            fs::create_directories(fs::path(path).parent_path());

            std::error_code error_code;
            if (!context->getSettingsRef().engine_file_truncate_on_insert && !is_path_with_globs
                && !FormatFactory::instance().checkIfFormatSupportAppend(format_name, context, format_settings)
                && fs::file_size(paths.back(), error_code) != 0 && !error_code)
            {
                if (context->getSettingsRef().engine_file_allow_create_multiple_files)
                {
                    auto pos = paths[0].find_first_of('.', paths[0].find_last_of('/'));
                    size_t index = paths.size();
                    String new_path;
                    do
                    {
                        new_path = paths[0].substr(0, pos) + "." + std::to_string(index) + (pos == std::string::npos ? "" : paths[0].substr(pos));
                        ++index;
                    }
                    while (fs::exists(new_path));
                    paths.push_back(new_path);
                    path = new_path;
                }
                else
                    throw Exception(
                        ErrorCodes::CANNOT_APPEND_TO_FILE,
                        "Cannot append data in format {} to file, because this format doesn't support appends."
                        " You can allow to create a new file "
                        "on each insert by enabling setting engine_file_allow_create_multiple_files",
                        format_name);
            }
        }

        return std::make_shared<StorageFileSink>(
            metadata_snapshot,
            getStorageID().getNameForLogs(),
            std::unique_lock{rwlock, getLockTimeout(context)},
            table_fd,
            use_table_fd,
            base_path,
            path,
            chooseCompressionMethod(path, compression_method),
            format_settings,
            format_name,
            context,
            flags);
    }
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
    if (is_path_with_globs)
        throw Exception("Can't truncate table '" + getStorageID().getNameForLogs() + "' in readonly mode", ErrorCodes::DATABASE_ACCESS_DENIED);

    if (use_table_fd)
    {
        if (0 != ::ftruncate(table_fd, 0))
            throwFromErrno("Cannot truncate file at fd " + toString(table_fd), ErrorCodes::CANNOT_TRUNCATE_FILE);
    }
    else
    {
        for (const auto & path : paths)
        {
            if (!fs::exists(path))
                continue;

            if (0 != ::truncate(path.c_str(), 0))
                throwFromErrnoWithPath("Cannot truncate file " + path, path, ErrorCodes::CANNOT_TRUNCATE_FILE);
        }
    }
}


void registerStorageFile(StorageFactory & factory)
{
    StorageFactory::StorageFeatures storage_features{
        .supports_settings = true,
        .supports_schema_inference = true,
        .source_access_type = AccessType::FILE,
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

            ASTList & engine_args_ast = factory_args.engine_args;

            if (!(engine_args_ast.size() >= 1 && engine_args_ast.size() <= 3)) // NOLINT
                throw Exception(
                    "Storage File requires from 1 to 3 arguments: name of used format, source and compression_method.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            auto it = engine_args_ast.begin();
            *it = evaluateConstantExpressionOrIdentifierAsLiteral(*it, factory_args.getLocalContext());
            storage_args.format_name = checkAndGetLiteralArgument<String>(*it, "format_name");

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
                return std::make_shared<StorageFile>(factory_args.relative_data_path, storage_args);

            /// Will use FD if engine_args[1] is int literal or identifier with std* name
            int source_fd = -1;
            String source_path;

            ++it;
            if (auto opt_name = tryGetIdentifierName(*it))
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
            else if (const auto * literal = (*it)->as<ASTLiteral>())
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

            ++it;
            if (engine_args_ast.size() == 3)
            {
                *it = evaluateConstantExpressionOrIdentifierAsLiteral(*it, factory_args.getLocalContext());
                storage_args.compression_method = checkAndGetLiteralArgument<String>(*it, "compression_method");
            }
            else
                storage_args.compression_method = "auto";

            if (0 <= source_fd) /// File descriptor
                return std::make_shared<StorageFile>(source_fd, storage_args);
            else /// User's file
                return std::make_shared<StorageFile>(source_path, factory_args.getContext()->getUserFilesPath(), storage_args);
        },
        storage_features);
}


NamesAndTypesList StorageFile::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}
}
