#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/PartitionedSink.h>
#include <Storages/Distributed/DistributedAsyncInsertSource.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/VirtualColumnUtils.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <IO/MMapReadBufferFromFile.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/IArchiveReader.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/ISource.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ResizeProcessor.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/parseGlobs.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <re2/re2.h>
#include <filesystem>
#include <shared_mutex>
#include <cmath>
#include <algorithm>

namespace ProfileEvents
{
    extern const Event CreatedReadBufferOrdinary;
    extern const Event CreatedReadBufferMMap;
    extern const Event CreatedReadBufferMMapFailed;
    extern const Event EngineFileLikeReadFiles;
}

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
    extern const int FILE_ALREADY_EXISTS;
    extern const int TIMEOUT_EXCEEDED;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int CANNOT_STAT;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_APPEND_TO_FILE;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{

/// Forward-declare to use in listFilesWithFoldedRegexpMatchingImpl()
void listFilesWithRegexpMatchingImpl(
    const std::string & path_for_ls,
    const std::string & for_match,
    size_t & total_bytes_to_read,
    std::vector<std::string> & result,
    bool recursive = false);

/*
 * When `{...}` has any `/`s, it must be processed in a different way:
 * Basically, a path with globs is processed by listFilesWithRegexpMatchingImpl. In case it detects multi-dir glob {.../..., .../...},
 * listFilesWithFoldedRegexpMatchingImpl is in charge from now on.
 * It works a bit different: it still recursively goes through subdirectories, but does not match every directory to glob.
 * Instead, it goes many levels down (until the approximate max_depth is reached) and compares this multi-dir path to a glob.
 * StorageHDFS.cpp has the same logic.
*/
void listFilesWithFoldedRegexpMatchingImpl(const std::string & path_for_ls,
                                           const std::string & processed_suffix,
                                           const std::string & suffix_with_globs,
                                           re2::RE2 & matcher,
                                           size_t & total_bytes_to_read,
                                           const size_t max_depth,
                                           const size_t next_slash_after_glob_pos,
                                           std::vector<std::string> & result)
{
    if (!max_depth)
        return;

    const fs::directory_iterator end;
    for (fs::directory_iterator it(path_for_ls); it != end; ++it)
    {
        const std::string full_path = it->path().string();
        const size_t last_slash = full_path.rfind('/');
        const String dir_or_file_name = full_path.substr(last_slash);

        if (re2::RE2::FullMatch(processed_suffix + dir_or_file_name, matcher))
        {
            if (next_slash_after_glob_pos == std::string::npos)
            {
                total_bytes_to_read += it->file_size();
                result.push_back(it->path().string());
            }
            else
            {
                listFilesWithRegexpMatchingImpl(fs::path(full_path) / "" ,
                                                suffix_with_globs.substr(next_slash_after_glob_pos),
                                                total_bytes_to_read, result);
            }
        }
        else if (it->is_directory())
        {
            listFilesWithFoldedRegexpMatchingImpl(fs::path(full_path), processed_suffix + dir_or_file_name,
                                                  suffix_with_globs, matcher, total_bytes_to_read,
                                                  max_depth - 1, next_slash_after_glob_pos, result);
        }

    }
}

/* Recursive directory listing with matched paths as a result.
 * Have the same method in StorageHDFS.
 */
void listFilesWithRegexpMatchingImpl(
    const std::string & path_for_ls,
    const std::string & for_match,
    size_t & total_bytes_to_read,
    std::vector<std::string> & result,
    bool recursive)
{
    const size_t first_glob_pos = for_match.find_first_of("*?{");
    const bool has_glob = first_glob_pos != std::string::npos;

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob_pos).rfind('/');
    const std::string suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'

    /// slashes_in_glob counter is a upper-bound estimate of recursion depth
    /// needed to process complex cases when `/` is included into glob, e.g. /pa{th1/a,th2/b}.csv
    size_t slashes_in_glob = 0;
    const size_t next_slash_after_glob_pos = [&]()
    {
        if (!has_glob)
            return suffix_with_globs.find('/', 1);

        size_t in_curly = 0;
        for (std::string::const_iterator it = ++suffix_with_globs.begin(); it != suffix_with_globs.end(); it++)
        {
            if (*it == '{')
                ++in_curly;
            else if (*it == '/')
            {
                if (in_curly)
                    ++slashes_in_glob;
                else
                    return size_t(std::distance(suffix_with_globs.begin(), it));
            }
            else if (*it == '}')
                --in_curly;
        }
        return std::string::npos;
    }();

    const std::string current_glob = suffix_with_globs.substr(0, next_slash_after_glob_pos);

    auto regexp = makeRegexpPatternFromGlobs(current_glob);

    re2::RE2 matcher(regexp);
    if (!matcher.ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
            "Cannot compile regex from glob ({}): {}", for_match, matcher.error());

    bool skip_regex = current_glob == "/*" ? true : false;
    if (!recursive)
        recursive = current_glob == "/**" ;

    const std::string prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs);

    if (!fs::exists(prefix_without_globs))
        return;

    const bool looking_for_directory = next_slash_after_glob_pos != std::string::npos;

    if (slashes_in_glob)
    {
        listFilesWithFoldedRegexpMatchingImpl(fs::path(prefix_without_globs), "", suffix_with_globs,
                                              matcher, total_bytes_to_read, slashes_in_glob,
                                              next_slash_after_glob_pos, result);
        return;
    }

    const fs::directory_iterator end;
    for (fs::directory_iterator it(prefix_without_globs); it != end; ++it)
    {
        const std::string full_path = it->path().string();
        const size_t last_slash = full_path.rfind('/');
        const String file_name = full_path.substr(last_slash);

        /// Condition is_directory means what kind of path is it in current iteration of ls
        if (!it->is_directory() && !looking_for_directory)
        {
            if (skip_regex || re2::RE2::FullMatch(file_name, matcher))
            {
                total_bytes_to_read += it->file_size();
                result.push_back(it->path().string());
            }
        }
        else if (it->is_directory())
        {
            if (recursive)
            {
                listFilesWithRegexpMatchingImpl(fs::path(full_path).append(it->path().string()) / "",
                                                looking_for_directory ? suffix_with_globs.substr(next_slash_after_glob_pos) : current_glob,
                                                total_bytes_to_read, result, recursive);
            }
            else if (looking_for_directory && re2::RE2::FullMatch(file_name, matcher))
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                listFilesWithRegexpMatchingImpl(fs::path(full_path) / "", suffix_with_globs.substr(next_slash_after_glob_pos), total_bytes_to_read, result);
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
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "File must not be a directory");
    }
}

std::unique_ptr<ReadBuffer> selectReadBuffer(
    const String & current_path,
    bool use_table_fd,
    int table_fd,
    const struct stat & file_stat,
    ContextPtr context)
{
    auto read_method = context->getSettingsRef().storage_file_read_method;

    /** Using mmap on server-side is unsafe for the following reasons:
      * - concurrent modifications of a file will result in SIGBUS;
      * - IO error from the device will result in SIGBUS;
      * - recovery from this signal is not feasible even with the usage of siglongjmp,
      *   as it might require stack unwinding from arbitrary place;
      * - arbitrary slowdown due to page fault in arbitrary place in the code is difficult to debug.
      *
      * But we keep this mode for clickhouse-local as it is not so bad for a command line tool.
      */
    if (context->getApplicationType() == Context::ApplicationType::SERVER && read_method == LocalFSReadMethod::mmap)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Using storage_file_read_method=mmap is not safe in server mode. Consider using pread.");

    if (S_ISREG(file_stat.st_mode) && read_method == LocalFSReadMethod::mmap)
    {
        try
        {
            std::unique_ptr<ReadBufferFromFileBase> res;
            if (use_table_fd)
                res = std::make_unique<MMapReadBufferFromFileDescriptor>(table_fd, 0);
            else
                res = std::make_unique<MMapReadBufferFromFile>(current_path, 0);

            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMap);
            return res;
        }
        catch (const ErrnoException &)
        {
            /// Fallback if mmap is not supported.
            ProfileEvents::increment(ProfileEvents::CreatedReadBufferMMapFailed);
        }
    }

    std::unique_ptr<ReadBufferFromFileBase> res;
    if (S_ISREG(file_stat.st_mode) && (read_method == LocalFSReadMethod::pread || read_method == LocalFSReadMethod::mmap))
    {
        if (use_table_fd)
            res = std::make_unique<ReadBufferFromFileDescriptorPRead>(table_fd);
        else
            res = std::make_unique<ReadBufferFromFilePRead>(current_path, context->getSettingsRef().max_read_buffer_size);

        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    }
    else
    {
        if (use_table_fd)
            res = std::make_unique<ReadBufferFromFileDescriptor>(table_fd);
        else
            res = std::make_unique<ReadBufferFromFile>(current_path, context->getSettingsRef().max_read_buffer_size);

        ProfileEvents::increment(ProfileEvents::CreatedReadBufferOrdinary);
    }
    return res;
}

struct stat getFileStat(const String & current_path, bool use_table_fd, int table_fd, const String & storage_name)
{
    struct stat file_stat{};
    if (use_table_fd)
    {
        /// Check if file descriptor allows random reads (and reading it twice).
        if (0 != fstat(table_fd, &file_stat))
            throwFromErrno("Cannot stat table file descriptor, inside " + storage_name, ErrorCodes::CANNOT_STAT);
    }
    else
    {
        /// Check if file descriptor allows random reads (and reading it twice).
        if (0 != stat(current_path.c_str(), &file_stat))
            throwFromErrno("Cannot stat file " + current_path, ErrorCodes::CANNOT_STAT);
    }

    return file_stat;
}

std::unique_ptr<ReadBuffer> createReadBuffer(
    const String & current_path,
    const struct stat & file_stat,
    bool use_table_fd,
    int table_fd,
    const String & compression_method,
    ContextPtr context,
    const String & path_to_archive = "")
{
    CompressionMethod method;

    if (!path_to_archive.empty())
    {
        auto reader = createArchiveReader(path_to_archive);

        if (current_path.find_first_of("*?{") != std::string::npos)
        {
            auto matcher = std::make_shared<re2::RE2>(makeRegexpPatternFromGlobs(current_path));
            if (!matcher->ok())
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "Cannot compile regex from glob ({}): {}", current_path, matcher->error());

            return reader->readFile([my_matcher = std::move(matcher)](const std::string & path)
            {
                return re2::RE2::FullMatch(path, *my_matcher);
            }, /*throw_on_not_found=*/true);
        }
        else
        {
            return reader->readFile(current_path, /*throw_on_not_found=*/true);
        }
    }

    if (use_table_fd)
        method = chooseCompressionMethod("", compression_method);
    else
        method = chooseCompressionMethod(current_path, compression_method);

    std::unique_ptr<ReadBuffer> nested_buffer = selectReadBuffer(current_path, use_table_fd, table_fd, file_stat, context);

    int zstd_window_log_max = static_cast<int>(context->getSettingsRef().zstd_window_log_max);
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

namespace
{
    struct ReadBufferFromFileIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferFromFileIterator(
            const std::vector<String> & paths_,
            const std::vector<String> & paths_to_archive_,
            const String & format_,
            const String & compression_method_,
            const std::optional<FormatSettings> & format_settings_,
            ContextPtr context_)
            : WithContext(context_)
            , paths(paths_)
            , paths_to_archive(paths_to_archive_)
            , format(format_)
            , compression_method(compression_method_)
            , format_settings(format_settings_)
        {
        }

        std::unique_ptr<ReadBuffer> next() override
        {

            String path;
            struct stat file_stat;
            bool is_first = current_index == 0;
            const auto & paths_ref = paths_to_archive.empty() ? paths : paths_to_archive;

            do
            {
                if (current_index == paths_ref.size())
                {
                    if (is_first)
                        throw Exception(
                            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                            "Cannot extract table structure from {} format file, because all files are empty. You must specify table structure manually",
                            format);
                    return nullptr;
                }

                path = paths_ref[current_index++];
                file_stat = getFileStat(path, false, -1, "File");
            } while (getContext()->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0);

            if (paths_to_archive.empty())
                return createReadBuffer(path, file_stat, false, -1, compression_method, getContext());

            return createReadBuffer(paths[0], file_stat, false, -1, compression_method, getContext(), path);
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().use_cache_for_count_from_files || !paths_to_archive.empty())
                return;

            auto key = getKeyForSchemaCache(paths[current_index - 1], format, format_settings, getContext());
            StorageFile::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

    private:
        const std::vector<String> & paths;
        const std::vector<String> & paths_to_archive;
        size_t current_index = 0;
        String format;
        String compression_method;
        const std::optional<FormatSettings> & format_settings;
    };
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
    auto file_stat = getFileStat("", true, table_fd, getName());
    /// We will use PeekableReadBuffer to create a checkpoint, so we need a place
    /// where we can store the original read buffer.
    read_buffer_from_fd = createReadBuffer("", file_stat, true, table_fd, compression_method, context);
    auto read_buf = std::make_unique<PeekableReadBuffer>(*read_buffer_from_fd);
    read_buf->setCheckpoint();
    auto read_buffer_iterator = SingleReadBufferIterator(std::move(read_buf));

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
    ContextPtr context,
    const std::vector<String> & paths_to_archive)
{
    if (format == "Distributed")
    {
        if (paths.empty())
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Cannot get table structure from file, because no files match specified name");

        return ColumnsDescription(DistributedAsyncInsertSource(paths[0]).getOutputs().front().getHeader().getNamesAndTypesList());
    }

    if (paths.empty() && !FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format))
        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "Cannot extract table structure from {} format file, because there are no files with provided path. "
            "You must specify table structure manually", format);

    std::optional<ColumnsDescription> columns_from_cache;
    if (context->getSettingsRef().schema_inference_use_cache_for_file)
        columns_from_cache = tryGetColumnsFromCache(paths, format, format_settings, context);

    ColumnsDescription columns;
    if (columns_from_cache)
    {
        columns = *columns_from_cache;
    }
    else
    {
        ReadBufferFromFileIterator read_buffer_iterator(paths, paths_to_archive, format, compression_method, format_settings, context);
        columns = readSchemaFromFormat(format, format_settings, read_buffer_iterator, paths.size() > 1, context);
    }

    if (context->getSettingsRef().schema_inference_use_cache_for_file)
        addColumnsToCache(paths, columns, format, format_settings, context);

    return columns;
}

bool StorageFile::supportsSubsetOfColumns() const
{
    return format_name != "Distributed" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name);
}

bool StorageFile::prefersLargeBlocks() const
{
    return FormatFactory::instance().checkIfOutputFormatPrefersLargeBlocks(format_name);
}

bool StorageFile::parallelizeOutputAfterReading(ContextPtr context) const
{
    return FormatFactory::instance().checkParallelizeOutputAfterReading(format_name, context);
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
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Using file descriptor as source of storage isn't allowed for server daemons");
    if (args.format_name == "Distributed")
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Distributed format is allowed only with explicit file path");

    is_db_table = false;
    use_table_fd = true;
    table_fd = table_fd_;
    setStorageMetadata(args);
}

StorageFile::StorageFile(const std::string & table_path_, const std::string & user_files_path, CommonArguments args)
    : StorageFile(args)
{
    if (!args.path_to_archive.empty())
    {
        paths_to_archive = getPathsList(args.path_to_archive, user_files_path, args.getContext(), total_bytes_to_read);
        paths = {table_path_};
    }
    else
    {
        paths = getPathsList(table_path_, user_files_path, args.getContext(), total_bytes_to_read);
    }

    is_db_table = false;
    is_path_with_globs = paths.size() > 1;
    if (!paths.empty())
        path_for_partitioned_write = paths.front();
    else
        path_for_partitioned_write = table_path_;

    file_renamer = FileRenamer(args.rename_after_processing);

    setStorageMetadata(args);
}

StorageFile::StorageFile(const std::string & relative_table_dir_path, CommonArguments args)
    : StorageFile(args)
{
    if (relative_table_dir_path.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Storage {} requires data path", getName());
    if (args.format_name == "Distributed")
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Distributed format is allowed only with explicit file path");

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
            columns = getTableStructureFromFile(format_name, paths, compression_method, format_settings, args.getContext(), paths_to_archive);
            if (!args.columns.empty() && args.columns != columns)
                throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Table structure and file structure are different");
        }
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(args.columns);

    storage_metadata.setConstraints(args.constraints);
    storage_metadata.setComment(args.comment);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathAndFileVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
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
    class FilesIterator
    {
    public:
        explicit FilesIterator(
            const Strings & files_,
            std::vector<std::string> archives_,
            const IArchiveReader::NameFilter & name_filter_,
            ASTPtr query,
            const NamesAndTypesList & virtual_columns,
            ContextPtr context_)
            : files(files_), archives(std::move(archives_)), name_filter(name_filter_)
        {
            ASTPtr filter_ast;
            if (archives.empty() && !files.empty() && !files[0].empty())
                filter_ast = VirtualColumnUtils::createPathAndFileFilterAst(query, virtual_columns, files[0], context_);

            if (filter_ast)
                VirtualColumnUtils::filterByPathOrFile(files, files, query, virtual_columns, context_, filter_ast);
        }

        String next()
        {
            const auto & fs = fromArchive() ? archives : files;

            auto current_index = index.fetch_add(1, std::memory_order_relaxed);
            if (current_index >= fs.size())
                return "";

            return fs[current_index];
        }

        bool fromArchive() const
        {
            return !archives.empty();
        }

        bool readSingleFileFromArchive() const
        {
            return !name_filter;
        }

        bool passesFilter(const std::string & name) const
        {
            std::lock_guard lock(filter_mutex);
            return name_filter(name);
        }

        const String & getFileName()
        {
            if (files.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected only 1 filename but got {}", files.size());

            return files[0];
        }
    private:
        std::vector<std::string> files;

        std::vector<std::string> archives;
        mutable std::mutex filter_mutex;
        IArchiveReader::NameFilter name_filter;

        std::atomic<size_t> index = 0;
    };

    using FilesIteratorPtr = std::shared_ptr<FilesIterator>;

    StorageFileSource(
        const ReadFromFormatInfo & info,
        std::shared_ptr<StorageFile> storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        const SelectQueryInfo & query_info_,
        UInt64 max_block_size_,
        FilesIteratorPtr files_iterator_,
        std::unique_ptr<ReadBuffer> read_buf_,
        bool need_only_count_)
        : ISource(info.source_header, false)
        , storage(std::move(storage_))
        , storage_snapshot(storage_snapshot_)
        , files_iterator(std::move(files_iterator_))
        , read_buf(std::move(read_buf_))
        , columns_description(info.columns_description)
        , requested_columns(info.requested_columns)
        , requested_virtual_columns(info.requested_virtual_columns)
        , block_for_format(info.format_header)
        , context(context_)
        , query_info(query_info_)
        , max_block_size(max_block_size_)
        , need_only_count(need_only_count_)
    {
        if (!storage->use_table_fd)
        {
            shared_lock = std::shared_lock(storage->rwlock, getLockTimeout(context));
            if (!shared_lock)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");
            storage->readers_counter.fetch_add(1, std::memory_order_release);
        }
    }


    /**
      * If specified option --rename_files_after_processing and files created by TableFunctionFile
      * Last reader will rename files according to specified pattern if desctuctor of reader was called without uncaught exceptions
      */
    void beforeDestroy()
    {
        if (storage->file_renamer.isEmpty())
            return;

        int32_t cnt = storage->readers_counter.fetch_sub(1, std::memory_order_acq_rel);

        if (std::uncaught_exceptions() == 0 && cnt == 1 && !storage->was_renamed)
        {
            shared_lock.unlock();
            auto exclusive_lock = std::unique_lock{storage->rwlock, getLockTimeout(context)};

            if (!exclusive_lock)
                return;
            if (storage->readers_counter.load(std::memory_order_acquire) != 0 || storage->was_renamed)
                return;

            for (auto & file_path_ref : storage->paths)
            {
                try
                {
                    auto file_path = fs::path(file_path_ref);
                    String new_filename = storage->file_renamer.generateNewFilename(file_path.filename().string());
                    file_path.replace_filename(new_filename);

                    // Normalize new path
                    file_path = file_path.lexically_normal();

                    // Checking access rights
                    checkCreationIsAllowed(context, context->getUserFilesPath(), file_path, true);

                    // Checking an existing of new file
                    if (fs::exists(file_path))
                        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File {} already exists", file_path.string());

                    fs::rename(fs::path(file_path_ref), file_path);
                    file_path_ref = file_path.string();
                    storage->was_renamed = true;
                }
                catch (const std::exception & e)
                {
                    // Cannot throw exception from destructor, will write only error
                    LOG_ERROR(&Poco::Logger::get("~StorageFileSource"), "Failed to rename file {}: {}", file_path_ref, e.what());
                    continue;
                }
            }
        }
    }

    ~StorageFileSource() override
    {
        beforeDestroy();
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
                    if (files_iterator->fromArchive())
                    {
                        if (files_iterator->readSingleFileFromArchive())
                        {
                            auto archive = files_iterator->next();
                            if (archive.empty())
                                return {};

                            struct stat file_stat = getFileStat(archive, storage->use_table_fd, storage->table_fd, storage->getName());
                            if (context->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0)
                                continue;

                            archive_reader = createArchiveReader(archive);
                            current_path = files_iterator->getFileName();
                            read_buf = archive_reader->readFile(current_path, /*throw_on_not_found=*/false);
                            if (!read_buf)
                                continue;
                        }
                        else
                        {
                            while (true)
                            {
                                if (file_enumerator == nullptr)
                                {
                                    auto archive = files_iterator->next();
                                    if (archive.empty())
                                        return {};

                                    struct stat file_stat = getFileStat(archive, storage->use_table_fd, storage->table_fd, storage->getName());
                                    if (context->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0)
                                        continue;

                                    archive_reader = createArchiveReader(archive);
                                    file_enumerator = archive_reader->firstFile();
                                    continue;
                                }

                                bool file_found = true;
                                while (!files_iterator->passesFilter(file_enumerator->getFileName()))
                                {
                                    if (!file_enumerator->nextFile())
                                    {
                                        file_found = false;
                                        break;
                                    }
                                }

                                if (file_found)
                                {
                                    current_path = file_enumerator->getFileName();
                                    break;
                                }

                                file_enumerator = nullptr;
                            }

                            chassert(file_enumerator);
                            read_buf = archive_reader->readFile(std::move(file_enumerator));
                        }
                    }
                    else
                    {
                        current_path = files_iterator->next();
                        if (current_path.empty())
                            return {};
                    }

                    /// Special case for distributed format. Defaults are not needed here.
                    if (storage->format_name == "Distributed")
                    {
                        pipeline = std::make_unique<QueryPipeline>(std::make_shared<DistributedAsyncInsertSource>(current_path));
                        reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
                        continue;
                    }
                }

                if (!read_buf)
                {
                    struct stat file_stat;
                    file_stat = getFileStat(current_path, storage->use_table_fd, storage->table_fd, storage->getName());

                    if (context->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0)
                        continue;

                    if (need_only_count && context->getSettingsRef().use_cache_for_count_from_files)
                    {
                        auto num_rows_from_cache = tryGetNumRowsFromCache(current_path, file_stat.st_mtime);
                        if (num_rows_from_cache)
                        {
                            /// We should not return single chunk with all number of rows,
                            /// because there is a chance that this chunk will be materialized later
                            /// (it can cause memory problems even with default values in columns or when virtual columns are requested).
                            /// Instead, we use special ConstChunkGenerator that will generate chunks
                            /// with max_block_size rows until total number of rows is reached.
                            auto const_chunk_generator = std::make_shared<ConstChunkGenerator>(block_for_format, *num_rows_from_cache, max_block_size);
                            QueryPipelineBuilder builder;
                            builder.init(Pipe(const_chunk_generator));
                            builder.addSimpleTransform([&](const Block & header)
                            {
                                return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
                            });
                            pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
                            reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
                            continue;
                        }
                    }

                    read_buf = createReadBuffer(current_path, file_stat, storage->use_table_fd, storage->table_fd, storage->compression_method, context);
                }

                const Settings & settings = context->getSettingsRef();
                chassert(!storage->paths.empty());
                const auto max_parsing_threads = std::max<size_t>(settings.max_threads/ storage->paths.size(), 1UL);
                input_format = context->getInputFormat(storage->format_name, *read_buf, block_for_format, max_block_size, storage->format_settings, need_only_count ? 1 : max_parsing_threads);
                input_format->setQueryInfo(query_info, context);
                if (need_only_count)
                    input_format->needOnlyCount();

                QueryPipelineBuilder builder;
                builder.init(Pipe(input_format));

                if (columns_description.hasDefaults())
                {
                    builder.addSimpleTransform([&](const Block & header)
                    {
                        return std::make_shared<AddingDefaultsTransform>(header, columns_description, *input_format, context);
                    });
                }

                /// Add ExtractColumnsTransform to extract requested columns/subcolumns
                /// from chunk read by IInputFormat.
                builder.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<ExtractColumnsTransform>(header, requested_columns);
                });

                pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
                reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

                ProfileEvents::increment(ProfileEvents::EngineFileLikeReadFiles);
            }

            Chunk chunk;
            if (reader->pull(chunk))
            {
                UInt64 num_rows = chunk.getNumRows();
                total_rows_in_file += num_rows;
                size_t chunk_size = 0;
                if (input_format && storage->format_name != "Distributed")
                    chunk_size = input_format->getApproxBytesReadForChunk();
                progress(num_rows, chunk_size ? chunk_size : chunk.bytes());

                /// Enrich with virtual columns.
                VirtualColumnUtils::addRequestedPathAndFileVirtualsToChunk(chunk, requested_virtual_columns, current_path);
                return chunk;
            }

            /// Read only once for file descriptor.
            if (storage->use_table_fd)
                finished_generate = true;

            if (input_format && storage->format_name != "Distributed" && context->getSettingsRef().use_cache_for_count_from_files)
                addNumRowsToCache(current_path, total_rows_in_file);

            total_rows_in_file = 0;

            /// Close file prematurely if stream was ended.
            reader.reset();
            pipeline.reset();
            input_format.reset();

            if (files_iterator->fromArchive() && !files_iterator->readSingleFileFromArchive())
                file_enumerator = archive_reader->nextFile(std::move(read_buf));

            read_buf.reset();
        }

        return {};
    }


private:
    void addNumRowsToCache(const String & path, size_t num_rows)
    {
        auto key = getKeyForSchemaCache(path, storage->format_name, storage->format_settings, context);
        StorageFile::getSchemaCache(context).addNumRows(key, num_rows);
    }

    std::optional<size_t> tryGetNumRowsFromCache(const String & path, time_t last_mod_time)
    {
        auto & schema_cache = StorageFile::getSchemaCache(context);
        auto key = getKeyForSchemaCache(path, storage->format_name, storage->format_settings, context);
        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            return last_mod_time;
        };

        return schema_cache.tryGetNumRows(key, get_last_mod_time);
    }

    std::shared_ptr<StorageFile> storage;
    StorageSnapshotPtr storage_snapshot;
    FilesIteratorPtr files_iterator;
    String current_path;
    Block sample_block;
    std::unique_ptr<ReadBuffer> read_buf;
    InputFormatPtr input_format;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;

    std::shared_ptr<IArchiveReader> archive_reader;
    std::unique_ptr<IArchiveReader::FileEnumerator> file_enumerator = nullptr;

    ColumnsDescription columns_description;
    NamesAndTypesList requested_columns;
    NamesAndTypesList requested_virtual_columns;
    Block block_for_format;

    ContextPtr context;    /// TODO Untangle potential issues with context lifetime.
    SelectQueryInfo query_info;
    UInt64 max_block_size;

    bool finished_generate = false;
    bool need_only_count = false;
    size_t total_rows_in_file = 0;

    std::shared_lock<std::shared_timed_mutex> shared_lock;
};


Pipe StorageFile::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    const size_t max_num_streams)
{
    if (use_table_fd)
    {
        paths = {""};   /// when use fd, paths are empty
    }
    else
    {
        const auto & p = paths_to_archive.empty() ? paths : paths_to_archive;
        if (p.size() == 1 && !fs::exists(p[0]))
        {
            if (context->getSettingsRef().engine_file_empty_if_not_exists)
                return Pipe(std::make_shared<NullSource>(storage_snapshot->getSampleBlockForColumns(column_names)));
            else
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist", p[0]);
        }
    }

    IArchiveReader::NameFilter filter;
    if (!paths_to_archive.empty())
    {
        if (paths.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Multiple paths defined for reading from archive");

        const auto & path = paths[0];

        if (path.find_first_of("*?{") != std::string::npos)
        {
            auto matcher = std::make_shared<re2::RE2>(makeRegexpPatternFromGlobs(path));
            if (!matcher->ok())
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "Cannot compile regex from glob ({}): {}", path, matcher->error());

            filter = [matcher](const std::string & p)
            {
                return re2::RE2::FullMatch(p, *matcher);
            };
        }
    }

    auto files_iterator = std::make_shared<StorageFileSource::FilesIterator>(paths, paths_to_archive, std::move(filter), query_info.query, virtual_columns, context);
    auto this_ptr = std::static_pointer_cast<StorageFile>(shared_from_this());

    size_t num_streams = max_num_streams;

    auto files_to_read = std::max(paths_to_archive.size(), paths.size());
    if (max_num_streams > files_to_read)
        num_streams = files_to_read;

    Pipes pipes;
    pipes.reserve(num_streams);

    /// Set total number of bytes to process. For progress bar.
    auto progress_callback = context->getFileProgressCallback();

    if (progress_callback)
        progress_callback(FileProgress(0, total_bytes_to_read));

    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(), getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && context->getSettingsRef().optimize_count_from_files;

    for (size_t i = 0; i < num_streams; ++i)
    {
        /// In case of reading from fd we have to check whether we have already created
        /// the read buffer from it in Storage constructor (for schema inference) or not.
        /// If yes, then we should use it in StorageFileSource. Atomic bool flag is needed
        /// to prevent data race in case of parallel reads.
        std::unique_ptr<ReadBuffer> read_buffer;
        if (has_peekable_read_buffer_from_fd.exchange(false))
            read_buffer = std::move(peekable_read_buffer_from_fd);

        pipes.emplace_back(std::make_shared<StorageFileSource>(
            read_from_format_info,
            this_ptr,
            storage_snapshot,
            context,
            query_info,
            max_block_size,
            files_iterator,
            std::move(read_buffer),
            need_only_count));
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
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");
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
            *write_buf, metadata_snapshot->getSampleBlock(), context, format_settings);

        if (do_not_write_prefix)
            writer->doNotWritePrefix();
    }

    String getName() const override { return "StorageFileSink"; }

    void consume(Chunk chunk) override
    {
        std::lock_guard cancel_lock(cancel_mutex);
        if (cancelled)
            return;
        writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onCancel() override
    {
        std::lock_guard cancel_lock(cancel_mutex);
        finalize();
        cancelled = true;
    }

    void onException(std::exception_ptr exception) override
    {
        std::lock_guard cancel_lock(cancel_mutex);
        try
        {
            std::rethrow_exception(exception);
        }
        catch (...)
        {
            /// An exception context is needed to proper delete write buffers without finalization
            release();
        }
    }

    void onFinish() override
    {
        std::lock_guard cancel_lock(cancel_mutex);
        finalize();
    }

private:
    void finalize()
    {
        if (!writer)
            return;

        try
        {
            writer->finalize();
            writer->flush();
            write_buf->finalize();
        }
        catch (...)
        {
            /// Stop ParallelFormattingOutputFormat correctly.
            release();
            throw;
        }
    }

    void release()
    {
        writer.reset();
        write_buf->finalize();
    }

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

    std::mutex cancel_mutex;
    bool cancelled = false;
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
    ContextPtr context,
    bool /*async_insert*/)
{
    if (!use_table_fd && !paths_to_archive.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writing to archives is not supported");

    if (format_name == "Distributed")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is not implemented for Distributed format");

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
                throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                                "Table '{}' is in readonly mode because of globs in filepath",
                                getStorageID().getNameForLogs());

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
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Table '{}' is in readonly mode", getStorageID().getNameForLogs());
    return paths;
}

void StorageFile::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    if (!is_db_table)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                        "Can't rename table {} bounded to user-defined file (or FD)", getStorageID().getNameForLogs());

    if (paths.size() != 1)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Can't rename table {} in readonly mode", getStorageID().getNameForLogs());

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
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Can't truncate table '{}' in readonly mode", getStorageID().getNameForLogs());

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
                {},
                {},
            };

            ASTs & engine_args_ast = factory_args.engine_args;

            if (!(engine_args_ast.size() >= 1 && engine_args_ast.size() <= 3)) // NOLINT
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Storage File requires from 1 to 3 arguments: "
                                "name of used format, source and compression_method.");

            engine_args_ast[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args_ast[0], factory_args.getLocalContext());
            storage_args.format_name = checkAndGetLiteralArgument<String>(engine_args_ast[0], "format_name");

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

            if (auto opt_name = tryGetIdentifierName(engine_args_ast[1]))
            {
                if (*opt_name == "stdin")
                    source_fd = STDIN_FILENO;
                else if (*opt_name == "stdout")
                    source_fd = STDOUT_FILENO;
                else if (*opt_name == "stderr")
                    source_fd = STDERR_FILENO;
                else
                    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier '{}' in second arg of File storage constructor",
                        *opt_name);
            }
            else if (const auto * literal = engine_args_ast[1]->as<ASTLiteral>())
            {
                auto type = literal->value.getType();
                if (type == Field::Types::Int64)
                    source_fd = static_cast<int>(literal->value.get<Int64>());
                else if (type == Field::Types::UInt64)
                    source_fd = static_cast<int>(literal->value.get<UInt64>());
                else if (type == Field::Types::String)
                    StorageFile::parseFileSource(literal->value.get<String>(), source_path, storage_args.path_to_archive);
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument must be path or file descriptor");
            }

            if (engine_args_ast.size() == 3)
            {
                engine_args_ast[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args_ast[2], factory_args.getLocalContext());
                storage_args.compression_method = checkAndGetLiteralArgument<String>(engine_args_ast[2], "compression_method");
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

SchemaCache & StorageFile::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_file", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
}

std::optional<ColumnsDescription> StorageFile::tryGetColumnsFromCache(
    const Strings & paths, const String & format_name, const std::optional<FormatSettings> & format_settings, ContextPtr context)
{
    /// Check if the cache contains one of the paths.
    auto & schema_cache = getSchemaCache(context);
    struct stat file_stat{};
    for (const auto & path : paths)
    {
        auto get_last_mod_time = [&]() -> std::optional<time_t>
        {
            if (0 != stat(path.c_str(), &file_stat))
                return std::nullopt;

            return file_stat.st_mtime;
        };

        auto cache_key = getKeyForSchemaCache(path, format_name, format_settings, context);
        auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void StorageFile::addColumnsToCache(
    const Strings & paths,
    const ColumnsDescription & columns,
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context)
{
    auto & schema_cache = getSchemaCache(context);
    auto cache_keys = getKeysForSchemaCache(paths, format_name, format_settings, context);
    schema_cache.addManyColumns(cache_keys, columns);
}

void StorageFile::parseFileSource(String source, String & filename, String & path_to_archive)
{
    size_t pos = source.find("::");
    if (pos == String::npos)
    {
        filename = std::move(source);
        return;
    }

    std::string_view path_to_archive_view = std::string_view{source}.substr(0, pos);
    while (path_to_archive_view.back() == ' ')
        path_to_archive_view.remove_suffix(1);

    if (path_to_archive_view.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path to archive is empty");

    path_to_archive = path_to_archive_view;

    std::string_view filename_view = std::string_view{source}.substr(pos + 2);
    while (filename_view.front() == ' ')
        filename_view.remove_prefix(1);

    if (filename_view.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filename is empty");

    filename = filename_view;
}

}
