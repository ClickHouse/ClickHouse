#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/PartitionedSink.h>
#include <Storages/Distributed/DistributedAsyncInsertSource.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/VirtualColumnUtils.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterSelectQuery.h>

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

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <Processors/SourceWithKeyCondition.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/parseGlobs.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/re2.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <shared_mutex>
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

    if (first_glob_pos == std::string::npos)
    {
        try
        {
            /// We use fs::canonical to resolve the canonical path and check if the file does exists
            /// but the result path will be fs::absolute.
            /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
            fs::canonical(path_for_ls + for_match);
            fs::path absolute_path = fs::absolute(path_for_ls + for_match);
            result.push_back(absolute_path.string());
        }
        catch (const std::exception &) // NOLINT
        {
            /// There is no such file, but we just ignore this.
            /// throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist", for_match);
        }
        return;
    }

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob_pos).rfind('/');
    const std::string suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'

    const size_t next_slash_after_glob_pos = suffix_with_globs.find('/', 1);

    const std::string current_glob = suffix_with_globs.substr(0, next_slash_after_glob_pos);

    auto regexp = makeRegexpPatternFromGlobs(current_glob);

    re2::RE2 matcher(regexp);
    if (!matcher.ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
            "Cannot compile regex from glob ({}): {}", for_match, matcher.error());

    bool skip_regex = current_glob == "/*";
    if (!recursive)
        recursive = current_glob == "/**" ;

    const std::string prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs);

    if (!fs::exists(prefix_without_globs))
        return;

    const bool looking_for_directory = next_slash_after_glob_pos != std::string::npos;

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
                listFilesWithRegexpMatchingImpl(fs::path(full_path) / "", suffix_with_globs.substr(next_slash_after_glob_pos),
                                                total_bytes_to_read, result, false);
        }
    }
}

std::vector<std::string> listFilesWithRegexpMatching(
    const std::string & for_match,
    size_t & total_bytes_to_read)
{
    std::vector<std::string> result;

    Strings for_match_paths_expanded = expandSelectionGlob(for_match);

    for (const auto & for_match_expanded : for_match_paths_expanded)
        listFilesWithRegexpMatchingImpl("/", for_match_expanded, total_bytes_to_read, result, false);

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
            throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat table file descriptor, inside {}", storage_name);
    }
    else
    {
        /// Check if file descriptor allows random reads (and reading it twice).
        if (0 != stat(current_path.c_str(), &file_stat))
            throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat file {}", current_path);
    }

    return file_stat;
}

std::unique_ptr<ReadBuffer> createReadBuffer(
    const String & current_path,
    const struct stat & file_stat,
    bool use_table_fd,
    int table_fd,
    const String & compression_method,
    ContextPtr context)
{
    CompressionMethod method;
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
        paths = listFilesWithRegexpMatching(path, total_bytes_to_read);
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
            const String & format_,
            const String & compression_method_,
            const std::optional<FormatSettings> & format_settings_,
            ContextPtr context_)
            : WithContext(context_)
            , paths(paths_)
            , format(format_)
            , compression_method(compression_method_)
            , format_settings(format_settings_)
        {
        }

        std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override
        {
            bool is_first = current_index == 0;
            /// For default mode check cached columns for all paths on first iteration.
            /// If we have cached columns, next() won't be called again.
            if (is_first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
            {
                if (auto cached_columns = tryGetColumnsFromCache(paths))
                    return {nullptr, cached_columns};
            }

            String path;
            struct stat file_stat;

            do
            {
                if (current_index == paths.size())
                {
                    if (is_first)
                        throw Exception(
                            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                            "Cannot extract table structure from {} format file, because all files are empty. You must specify table structure manually",
                            format);
                    return {nullptr, std::nullopt};
                }

                path = paths[current_index++];
                file_stat = getFileStat(path, false, -1, "File");
            } while (getContext()->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0);

            /// For union mode, check cached columns only for current path, because schema can be different for different files.
            if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
            {
                if (auto cached_columns = tryGetColumnsFromCache({path}))
                    return {nullptr, cached_columns};
            }

            return {createReadBuffer(path, file_stat, false, -1, compression_method, getContext()), std::nullopt};
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().use_cache_for_count_from_files)
                return;

            auto key = getKeyForSchemaCache(paths[current_index - 1], format, format_settings, getContext());
            StorageFile::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

        void setSchemaToLastFile(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_file
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::UNION)
                return;

            /// For union mode, schema can be different for different files, so we need to
            /// cache last inferred schema only for last processed file.
            auto cache_key = getKeyForSchemaCache(paths[current_index - 1], format, format_settings, getContext());
            StorageFile::getSchemaCache(getContext()).addColumns(cache_key, columns);
        }

        void setResultingSchema(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_file
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::DEFAULT)
                return;

            /// For default mode we cache resulting schema for all paths.
            auto cache_keys = getKeysForSchemaCache(paths, format, format_settings, getContext());
            StorageFile::getSchemaCache(getContext()).addManyColumns(cache_keys, columns);
        }

        String getLastFileName() const override
        {
            if (current_index != 0)
                return paths[current_index - 1];
            return "";
        }

    private:
        std::optional<ColumnsDescription> tryGetColumnsFromCache(const Strings & paths_)
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_file)
                return std::nullopt;

            /// Check if the cache contains one of the paths.
            auto & schema_cache = StorageFile::getSchemaCache(getContext());
            struct stat file_stat{};
            for (const auto & path : paths_)
            {
                auto get_last_mod_time = [&]() -> std::optional<time_t>
                {
                    if (0 != stat(path.c_str(), &file_stat))
                        return std::nullopt;

                    return file_stat.st_mtime;
                };

                auto cache_key = getKeyForSchemaCache(path, format, format_settings, getContext());
                auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
                if (columns)
                    return columns;
            }

            return std::nullopt;
        }

        const std::vector<String> & paths;

        size_t current_index = 0;
        String format;
        String compression_method;
        const std::optional<FormatSettings> & format_settings;
    };

    struct ReadBufferFromArchiveIterator : public IReadBufferIterator, WithContext
    {
    public:
        ReadBufferFromArchiveIterator(
            const StorageFile::ArchiveInfo & archive_info_,
            const String & format_,
            const std::optional<FormatSettings> & format_settings_,
            ContextPtr context_)
            : WithContext(context_)
            , archive_info(archive_info_)
            , format(format_)
            , format_settings(format_settings_)
        {
        }

        std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override
        {
            /// For default mode check cached columns for all initial archive paths (maybe with globs) on first iteration.
            /// If we have cached columns, next() won't be called again.
            if (is_first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
            {
                for (const auto & archive : archive_info.paths_to_archives)
                {
                    if (auto cached_columns = tryGetColumnsFromSchemaCache(archive, archive_info.path_in_archive))
                        return {nullptr, cached_columns};
                }
            }

            std::unique_ptr<ReadBuffer> read_buf;
            while (true)
            {
                if (current_archive_index == archive_info.paths_to_archives.size())
                {
                    if (is_first)
                        throw Exception(
                            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                            "Cannot extract table structure from {} format file, because all files are empty. You must specify table structure manually",
                            format);

                    return {nullptr, std::nullopt};
                }

                const auto & archive = archive_info.paths_to_archives[current_archive_index];
                struct stat file_stat;
                file_stat = getFileStat(archive, false, -1, "File");
                if (file_stat.st_size == 0)
                {
                    if (getContext()->getSettingsRef().engine_file_skip_empty_files)
                    {
                        ++current_archive_index;
                        continue;
                    }

                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "Cannot extract table structure from {} format file, because the archive {} is empty. "
                        "You must specify table structure manually",
                        format,
                        archive);
                }

                auto archive_reader = createArchiveReader(archive);

                if (archive_info.isSingleFileRead())
                {
                    read_buf = archive_reader->readFile(archive_info.path_in_archive, false);
                    ++current_archive_index;
                    if (!read_buf)
                        continue;

                    last_read_file_path = paths_for_schema_cache.emplace_back(fmt::format("{}::{}", archive_reader->getPath(), archive_info.path_in_archive));
                    is_first = false;

                    if (auto cached_columns = tryGetColumnsFromSchemaCache(archive, last_read_file_path))
                        return {nullptr, cached_columns};
                }
                else
                {
                    if (last_read_buffer)
                        file_enumerator = archive_reader->nextFile(std::move(last_read_buffer));
                    else
                        file_enumerator = archive_reader->firstFile();

                    if (!file_enumerator)
                    {
                        ++current_archive_index;
                        continue;
                    }

                    const auto * filename = &file_enumerator->getFileName();
                    while (!archive_info.filter(*filename))
                    {
                        if (!file_enumerator->nextFile())
                        {
                            archive_reader = nullptr;
                            break;
                        }

                        filename = &file_enumerator->getFileName();
                    }

                    if (!archive_reader)
                    {
                        ++current_archive_index;
                        continue;
                    }

                    last_read_file_path = paths_for_schema_cache.emplace_back(fmt::format("{}::{}", archive_reader->getPath(), *filename));
                    is_first = false;

                    if (auto cached_columns = tryGetColumnsFromSchemaCache(archive, last_read_file_path))
                    {
                        /// For union mode next() will be called again even if we found cached columns,
                        /// so we need to remember last_read_buffer to continue iterating through files in archive.
                        if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
                            last_read_buffer = archive_reader->readFile(std::move(file_enumerator));
                        return {nullptr, cached_columns};
                    }

                    read_buf = archive_reader->readFile(std::move(file_enumerator));
                }

                break;
            }

            return {std::move(read_buf), std::nullopt};
        }

        void setPreviousReadBuffer(std::unique_ptr<ReadBuffer> buffer) override
        {
            if (buffer)
                last_read_buffer = std::move(buffer);
        }

        void setNumRowsToLastFile(size_t num_rows) override
        {
            if (!getContext()->getSettingsRef().use_cache_for_count_from_files)
                return;

            auto key = getKeyForSchemaCache(last_read_file_path, format, format_settings, getContext());
            StorageFile::getSchemaCache(getContext()).addNumRows(key, num_rows);
        }

        void setSchemaToLastFile(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_file
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::UNION)
                return;

            /// For union mode, schema can be different for different files in archive, so we need to
            /// cache last inferred schema only for last processed file.
            auto & schema_cache = StorageFile::getSchemaCache(getContext());
            auto cache_key = getKeyForSchemaCache(last_read_file_path, format, format_settings, getContext());
            schema_cache.addColumns(cache_key, columns);
        }

        void setResultingSchema(const ColumnsDescription & columns) override
        {
            if (!getContext()->getSettingsRef().schema_inference_use_cache_for_file
                || getContext()->getSettingsRef().schema_inference_mode != SchemaInferenceMode::DEFAULT)
                return;

            /// For default mode we cache resulting schema for all paths.
            /// Also add schema for initial paths (maybe with globes) in cache,
            /// so next time we won't iterate through files (that can be expensive).
            for (const auto & archive : archive_info.paths_to_archives)
                paths_for_schema_cache.emplace_back(fmt::format("{}::{}", archive, archive_info.path_in_archive));
            auto & schema_cache = StorageFile::getSchemaCache(getContext());
            auto cache_keys = getKeysForSchemaCache(paths_for_schema_cache, format, format_settings, getContext());
            schema_cache.addManyColumns(cache_keys, columns);
        }

        String getLastFileName() const override
        {
            return last_read_file_path;
        }

    private:
        std::optional<ColumnsDescription> tryGetColumnsFromSchemaCache(const std::string & archive_path, const std::string & full_path)
        {
            auto context = getContext();
            if (!context->getSettingsRef().schema_inference_use_cache_for_file)
                return std::nullopt;

            struct stat file_stat;
            auto & schema_cache = StorageFile::getSchemaCache(context);
            auto get_last_mod_time = [&]() -> std::optional<time_t>
            {
                if (0 != stat(archive_path.c_str(), &file_stat))
                    return std::nullopt;

                return file_stat.st_mtime;
            };

            auto cache_key = getKeyForSchemaCache(full_path, format, format_settings, context);
            auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);

            if (columns)
                return columns;

            return std::nullopt;
        }

        const StorageFile::ArchiveInfo & archive_info;

        size_t current_archive_index = 0;

        bool is_first = true;

        std::string last_read_file_path;

        std::unique_ptr<IArchiveReader::FileEnumerator> file_enumerator;
        std::unique_ptr<ReadBuffer> last_read_buffer;

        String format;
        const std::optional<FormatSettings> & format_settings;
        std::vector<std::string> paths_for_schema_cache;
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
    const std::optional<ArchiveInfo> & archive_info)
{
    if (format == "Distributed")
    {
        if (paths.empty())
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Cannot get table structure from file, because no files match specified name");

        return ColumnsDescription(DistributedAsyncInsertSource(paths[0]).getOutputs().front().getHeader().getNamesAndTypesList());
    }

    if (((archive_info && archive_info->paths_to_archives.empty()) || (!archive_info && paths.empty()))
        && !FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format))
        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "Cannot extract table structure from {} format file, because there are no files with provided path. "
            "You must specify table structure manually", format);

    if (archive_info)
    {
        ReadBufferFromArchiveIterator read_buffer_iterator(*archive_info, format, format_settings, context);
        return readSchemaFromFormat(
            format,
            format_settings,
            read_buffer_iterator,
            /*retry=*/archive_info->paths_to_archives.size() > 1 || !archive_info->isSingleFileRead(),
            context);
    }

    ReadBufferFromFileIterator read_buffer_iterator(paths, format, compression_method, format_settings, context);
    return readSchemaFromFormat(format, format_settings, read_buffer_iterator, paths.size() > 1, context);
}

bool StorageFile::supportsSubsetOfColumns(const ContextPtr & context) const
{
    return format_name != "Distributed" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format_name, context, format_settings);
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
        throw ErrnoException(ErrorCodes::CANNOT_FSTAT, "Cannot execute fstat");
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
        archive_info = getArchiveInfo(args.path_to_archive, table_path_, user_files_path, args.getContext(), total_bytes_to_read);
    else
        paths = getPathsList(table_path_, user_files_path, args.getContext(), total_bytes_to_read);

    is_db_table = false;
    is_path_with_globs = paths.size() > 1;
    if (!paths.empty())
        path_for_partitioned_write = paths.front();
    else
        path_for_partitioned_write = table_path_;

    file_renamer = FileRenamer(args.rename_after_processing);

    setStorageMetadata(args);
}

StorageFile::StorageFile(const std::string & table_path_, const std::string & user_files_path, bool distributed_processing_, CommonArguments args)
    : StorageFile(table_path_, user_files_path, args)
{
    distributed_processing = distributed_processing_;
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
            columns = getTableStructureFromFile(
                format_name,
                paths,
                compression_method,
                format_settings,
                args.getContext(),
                archive_info);
            if (!args.columns.empty() && args.columns != columns)
                throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Table structure and file structure are different");
        }
        storage_metadata.setColumns(columns);
    }
    else
    {
        /// We don't allow special columns in File storage.
        if (!args.columns.hasOnlyOrdinary())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table engine File doesn't support special columns like MATERIALIZED, ALIAS or EPHEMERAL");
        storage_metadata.setColumns(args.columns);
    }

    storage_metadata.setConstraints(args.constraints);
    storage_metadata.setComment(args.comment);
    setInMemoryMetadata(storage_metadata);

    virtual_columns = VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage(storage_metadata.getSampleBlock().getNamesAndTypesList());
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

StorageFileSource::FilesIterator::FilesIterator(
    const Strings & files_,
    std::optional<StorageFile::ArchiveInfo> archive_info_,
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    ContextPtr context_,
    bool distributed_processing_)
    : files(files_), archive_info(std::move(archive_info_)), distributed_processing(distributed_processing_), context(context_)
{
    ActionsDAGPtr filter_dag;
    if (!distributed_processing && !archive_info && !files.empty())
        filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate, virtual_columns);

    if (filter_dag)
        VirtualColumnUtils::filterByPathOrFile(files, files, filter_dag, virtual_columns, context_);
}

String StorageFileSource::FilesIterator::next()
{
    if (distributed_processing)
        return context->getReadTaskCallback()();
    else
    {
        const auto & fs = isReadFromArchive() ? archive_info->paths_to_archives : files;

        auto current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= fs.size())
            return {};

        return fs[current_index];
    }
}

const String & StorageFileSource::FilesIterator::getFileNameInArchive()
{
    if (archive_info->path_in_archive.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected only 1 filename but it's empty");

    return archive_info->path_in_archive;
}

StorageFileSource::StorageFileSource(
    const ReadFromFormatInfo & info,
    std::shared_ptr<StorageFile> storage_,
    ContextPtr context_,
    UInt64 max_block_size_,
    FilesIteratorPtr files_iterator_,
    std::unique_ptr<ReadBuffer> read_buf_,
    bool need_only_count_)
    : SourceWithKeyCondition(info.source_header, false)
    , storage(std::move(storage_))
    , files_iterator(std::move(files_iterator_))
    , read_buf(std::move(read_buf_))
    , columns_description(info.columns_description)
    , requested_columns(info.requested_columns)
    , requested_virtual_columns(info.requested_virtual_columns)
    , block_for_format(info.format_header)
    , context(context_)
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

void StorageFileSource::beforeDestroy()
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
                LOG_ERROR(getLogger("~StorageFileSource"), "Failed to rename file {}: {}", file_path_ref, e.what());
                continue;
            }
        }
    }
}

StorageFileSource::~StorageFileSource()
{
    beforeDestroy();
}

void StorageFileSource::setKeyCondition(const ActionsDAG::NodeRawConstPtrs & nodes, ContextPtr context_)
{
    setKeyConditionImpl(nodes, context_, block_for_format);
}


bool StorageFileSource::tryGetCountFromCache(const struct stat & file_stat)
{
    if (!context->getSettingsRef().use_cache_for_count_from_files)
        return false;

    auto num_rows_from_cache = tryGetNumRowsFromCache(current_path, file_stat.st_mtime);
    if (!num_rows_from_cache)
        return false;

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
    return true;
}

Chunk StorageFileSource::generate()
{
    while (!finished_generate)
    {
        /// Open file lazily on first read. This is needed to avoid too many open files from different streams.
        if (!reader)
        {
            if (!storage->use_table_fd)
            {
                if (files_iterator->isReadFromArchive())
                {
                    if (files_iterator->isSingleFileReadFromArchive())
                    {
                        auto archive = files_iterator->next();
                        if (archive.empty())
                            return {};

                        auto file_stat = getFileStat(archive, storage->use_table_fd, storage->table_fd, storage->getName());
                        if (context->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0)
                            continue;

                        archive_reader = createArchiveReader(archive);
                        filename_override = files_iterator->getFileNameInArchive();

                        current_path = fmt::format("{}::{}", archive_reader->getPath(), *filename_override);
                        if (need_only_count && tryGetCountFromCache(file_stat))
                            continue;

                        read_buf = archive_reader->readFile(*filename_override, /*throw_on_not_found=*/false);
                        if (!read_buf)
                            continue;

                        if (auto progress_callback = context->getFileProgressCallback())
                            progress_callback(FileProgress(0, tryGetFileSizeFromReadBuffer(*read_buf).value_or(0)));
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

                                current_archive_stat = getFileStat(archive, storage->use_table_fd, storage->table_fd, storage->getName());
                                if (context->getSettingsRef().engine_file_skip_empty_files && current_archive_stat.st_size == 0)
                                    continue;

                                archive_reader = createArchiveReader(archive);
                                file_enumerator = archive_reader->firstFile();
                                continue;
                            }

                            bool file_found = true;
                            while (!files_iterator->validFileInArchive(file_enumerator->getFileName()))
                            {
                                if (!file_enumerator->nextFile())
                                {
                                    file_found = false;
                                    break;
                                }
                            }

                            if (file_found)
                            {
                                filename_override = file_enumerator->getFileName();
                                break;
                            }

                            file_enumerator = nullptr;
                        }

                        chassert(file_enumerator);
                        current_path = fmt::format("{}::{}", archive_reader->getPath(), *filename_override);
                        current_file_size = file_enumerator->getFileInfo().uncompressed_size;
                        if (need_only_count && tryGetCountFromCache(current_archive_stat))
                            continue;

                        read_buf = archive_reader->readFile(std::move(file_enumerator));
                        if (auto progress_callback = context->getFileProgressCallback())
                            progress_callback(FileProgress(0, tryGetFileSizeFromReadBuffer(*read_buf).value_or(0)));
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
                current_file_size = file_stat.st_size;

                if (context->getSettingsRef().engine_file_skip_empty_files && file_stat.st_size == 0)
                    continue;

                if (need_only_count && tryGetCountFromCache(file_stat))
                    continue;

                read_buf = createReadBuffer(current_path, file_stat, storage->use_table_fd, storage->table_fd, storage->compression_method, context);
            }

            const Settings & settings = context->getSettingsRef();

            size_t file_num = 0;
            if (storage->archive_info)
                file_num = storage->archive_info->paths_to_archives.size();
            else
                file_num = storage->paths.size();

            chassert(file_num > 0);

            const auto max_parsing_threads = std::max<size_t>(settings.max_threads / file_num, 1UL);
            input_format = FormatFactory::instance().getInput(
                storage->format_name, *read_buf, block_for_format, context, max_block_size, storage->format_settings,
                max_parsing_threads, std::nullopt, /*is_remote_fs*/ false, CompressionMethod::None, need_only_count);

            if (key_condition)
                input_format->setKeyCondition(key_condition);

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
            VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(
                chunk, requested_virtual_columns, current_path, current_file_size, filename_override.has_value() ? &filename_override.value() : nullptr);
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

        if (files_iterator->isReadFromArchive() && !files_iterator->isSingleFileReadFromArchive())
        {
            if (file_enumerator)
            {
                if (!file_enumerator->nextFile())
                    file_enumerator = nullptr;
            }
            else
            {
                file_enumerator = archive_reader->nextFile(std::move(read_buf));
            }
        }

        read_buf.reset();
    }

    return {};
}

void StorageFileSource::addNumRowsToCache(const String & path, size_t num_rows) const
{
    auto key = getKeyForSchemaCache(path, storage->format_name, storage->format_settings, context);
    StorageFile::getSchemaCache(context).addNumRows(key, num_rows);
}

std::optional<size_t> StorageFileSource::tryGetNumRowsFromCache(const String & path, time_t last_mod_time) const
{
    auto & schema_cache = StorageFile::getSchemaCache(context);
    auto key = getKeyForSchemaCache(path, storage->format_name, storage->format_settings, context);
    auto get_last_mod_time = [&]() -> std::optional<time_t>
    {
        return last_mod_time;
    };

    return schema_cache.tryGetNumRows(key, get_last_mod_time);
}

class ReadFromFile : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromFile"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters() override;

    ReadFromFile(
        Block sample_block,
        std::shared_ptr<StorageFile> storage_,
        ReadFromFormatInfo info_,
        const bool need_only_count_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)})
        , storage(std::move(storage_))
        , info(std::move(info_))
        , need_only_count(need_only_count_)
        , context(std::move(context_))
        , max_block_size(max_block_size_)
        , max_num_streams(num_streams_)
    {
    }

private:
    std::shared_ptr<StorageFile> storage;
    ReadFromFormatInfo info;
    const bool need_only_count;

    ContextPtr context;
    size_t max_block_size;
    const size_t max_num_streams;

    std::shared_ptr<StorageFileSource::FilesIterator> files_iterator;

    void createIterator(const ActionsDAG::Node * predicate);
};

void ReadFromFile::applyFilters()
{
    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    createIterator(predicate);
}

void StorageFile::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    if (use_table_fd)
    {
        paths = {""};   /// when use fd, paths are empty
    }
    else
    {
        const std::vector<std::string> * p;

        if (archive_info.has_value())
            p = &archive_info->paths_to_archives;
        else
            p = &paths;

        if (p->size() == 1 && !fs::exists(p->at(0)))
        {
            if (!context->getSettingsRef().engine_file_empty_if_not_exists)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist", p->at(0));

            auto header = storage_snapshot->getSampleBlockForColumns(column_names);
            InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info);
            return;
        }
    }

    auto this_ptr = std::static_pointer_cast<StorageFile>(shared_from_this());

    auto read_from_format_info = prepareReadingFromFormat(column_names, storage_snapshot, supportsSubsetOfColumns(context), getVirtuals());
    bool need_only_count = (query_info.optimize_trivial_count || read_from_format_info.requested_columns.empty())
        && context->getSettingsRef().optimize_count_from_files;

    auto reading = std::make_unique<ReadFromFile>(
        read_from_format_info.source_header,
        std::move(this_ptr),
        std::move(read_from_format_info),
        need_only_count,
        context,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromFile::createIterator(const ActionsDAG::Node * predicate)
{
    if (files_iterator)
        return;

    files_iterator = std::make_shared<StorageFileSource::FilesIterator>(
        storage->paths,
        storage->archive_info,
        predicate,
        storage->virtual_columns,
        context,
        storage->distributed_processing);
}

void ReadFromFile::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    createIterator(nullptr);

    size_t num_streams = max_num_streams;

    size_t files_to_read = 0;
    if (storage->archive_info)
        files_to_read = storage->archive_info->paths_to_archives.size();
    else
        files_to_read = storage->paths.size();

    if (max_num_streams > files_to_read)
        num_streams = files_to_read;

    Pipes pipes;
    pipes.reserve(num_streams);

    /// Set total number of bytes to process. For progress bar.
    auto progress_callback = context->getFileProgressCallback();

    if (progress_callback && !storage->archive_info)
        progress_callback(FileProgress(0, storage->total_bytes_to_read));

    for (size_t i = 0; i < num_streams; ++i)
    {
        /// In case of reading from fd we have to check whether we have already created
        /// the read buffer from it in Storage constructor (for schema inference) or not.
        /// If yes, then we should use it in StorageFileSource. Atomic bool flag is needed
        /// to prevent data race in case of parallel reads.
        std::unique_ptr<ReadBuffer> read_buffer;
        if (storage->has_peekable_read_buffer_from_fd.exchange(false))
            read_buffer = std::move(storage->peekable_read_buffer_from_fd);

        auto source = std::make_shared<StorageFileSource>(
            info,
            storage,
            context,
            max_block_size,
            files_iterator,
            std::move(read_buffer),
            need_only_count);

        source->setKeyCondition(filter_nodes.nodes, context);
        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef().parallelize_output_from_storages;
    if (parallelize_output && storage->parallelizeOutputAfterReading(context) && output_ports > 0 && output_ports < max_num_streams)
        pipe.resize(max_num_streams);

    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(info.source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
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
        const auto & settings = context->getSettingsRef();
        write_buf = wrapWriteBufferWithCompressionMethod(
            std::move(naked_buffer),
            compression_method,
            static_cast<int>(settings.output_format_compression_level),
            static_cast<int>(settings.output_format_compression_zstd_window_log));

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

        fs::create_directories(fs::path(partition_path).parent_path());

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
    if (!use_table_fd && archive_info.has_value())
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
            throw ErrnoException(ErrorCodes::CANNOT_TRUNCATE_FILE, "Cannot truncate file at fd {}", toString(table_fd));
    }
    else
    {
        for (const auto & path : paths)
        {
            if (!fs::exists(path))
                continue;

            if (0 != ::truncate(path.c_str(), 0))
                ErrnoException::throwFromPath(ErrorCodes::CANNOT_TRUNCATE_FILE, path, "Cannot truncate file at {}", path);
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
                return std::make_shared<StorageFile>(source_path, factory_args.getContext()->getUserFilesPath(), false, storage_args);
        },
        storage_features);
}

SchemaCache & StorageFile::getSchemaCache(const ContextPtr & context)
{
    static SchemaCache schema_cache(context->getConfigRef().getUInt("schema_inference_cache_max_elements_for_file", DEFAULT_SCHEMA_CACHE_ELEMENTS));
    return schema_cache;
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
    while (path_to_archive_view.ends_with(' '))
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

StorageFile::ArchiveInfo StorageFile::getArchiveInfo(
    const std::string & path_to_archive,
    const std::string & file_in_archive,
    const std::string & user_files_path,
    ContextPtr context,
    size_t & total_bytes_to_read
)
{
    ArchiveInfo archive_info;
    archive_info.path_in_archive = file_in_archive;

    if (file_in_archive.find_first_of("*?{") != std::string::npos)
    {
        auto matcher = std::make_shared<re2::RE2>(makeRegexpPatternFromGlobs(file_in_archive));
        if (!matcher->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile regex from glob ({}): {}", file_in_archive, matcher->error());

        archive_info.filter = [matcher, matcher_mutex = std::make_shared<std::mutex>()](const std::string & p) mutable
        {
            std::lock_guard lock(*matcher_mutex);
            return re2::RE2::FullMatch(p, *matcher);
        };
    }

    archive_info.paths_to_archives = getPathsList(path_to_archive, user_files_path, context, total_bytes_to_read);

    return archive_info;
}

Names StorageFile::getVirtualColumnNames()
{
    return VirtualColumnUtils::getPathFileAndSizeVirtualsForStorage({}).getNames();
}

}
