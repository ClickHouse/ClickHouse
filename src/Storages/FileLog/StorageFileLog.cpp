#include <Core/Settings.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/StoragePolicy.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteIntText.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/FileLog/FileLogSettings.h>
#include <Storages/FileLog/FileLogSource.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <base/errnoToString.h>

#include <sys/stat.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsMilliseconds stream_poll_timeout_ms;
    extern const SettingsBool use_concurrency_control;
}

namespace FileLogSetting
{
    extern const FileLogSettingsStreamingHandleErrorMode handle_error_mode;
    extern const FileLogSettingsUInt64 max_block_size;
    extern const FileLogSettingsMaxThreads max_threads;
    extern const FileLogSettingsUInt64 poll_directory_watch_events_backoff_factor;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_init;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_max;
    extern const FileLogSettingsUInt64 poll_max_batch_size;
    extern const FileLogSettingsMilliseconds poll_timeout_ms;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_STAT;
    extern const int BAD_FILE_TYPE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_METADATA_ALREADY_EXISTS;
    extern const int CANNOT_SELECT;
    extern const int QUERY_NOT_ALLOWED;
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{
    const auto MAX_THREAD_WORK_DURATION_MS = 60000;

    ContextMutablePtr configureContext(ContextPtr context)
    {
        auto new_context = Context::createCopy(context);
        /// It does not make sense to use auto detection here, since the format
        /// will be reset for each message, plus, auto detection takes CPU
        /// time.
        new_context->setSetting("input_format_csv_detect_header", false);
        new_context->setSetting("input_format_tsv_detect_header", false);
        new_context->setSetting("input_format_custom_detect_header", false);
        return new_context;
    }
}

static constexpr auto TMP_SUFFIX = ".tmp";


class ReadFromStorageFileLog final : public ReadFromStreamLikeEngine
{
public:
    ReadFromStorageFileLog(
        const Names & column_names_,
        StoragePtr storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SelectQueryInfo & query_info,
        ContextPtr context_)
        : ReadFromStreamLikeEngine{column_names_, storage_snapshot_, query_info.storage_limits, context_}
        , column_names{column_names_}
        , storage{storage_}
        , storage_snapshot{storage_snapshot_}
    {
    }

    String getName() const override { return "ReadFromStorageFileLog"; }

private:
    Pipe makePipe() final
    {
        auto & file_log = storage->as<StorageFileLog &>();
        if (file_log.mv_attached)
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageFileLog with attached materialized views");

        std::lock_guard lock(file_log.file_infos_mutex);
        if (file_log.running_streams)
            throw Exception(ErrorCodes::CANNOT_SELECT, "Another select query is running on this table, need to wait it finish.");

        file_log.updateFileInfos();

        /// No files to parse
        if (file_log.file_infos.file_names.empty())
        {
            LOG_WARNING(file_log.log, "There is a idle table named {}, no files need to parse.", getName());
            Block header;
            auto column_names_and_types = storage_snapshot->getColumnsByNames(GetColumnsOptions::All, column_names);
            for (const auto & [name, type] : column_names_and_types)
                header.insert(ColumnWithTypeAndName(type, name));
            return Pipe(std::make_unique<NullSource>(std::make_shared<const Block>(header)));
        }

        auto modified_context = Context::createCopy(file_log.filelog_context);

        auto max_streams_number = std::min<UInt64>((*file_log.filelog_settings)[FileLogSetting::max_threads], file_log.file_infos.file_names.size());

        /// Each stream responsible for closing it's files and store meta
        file_log.openFilesAndSetPos();

        Pipes pipes;
        pipes.reserve(max_streams_number);
        for (size_t stream_number = 0; stream_number < max_streams_number; ++stream_number)
        {
            pipes.emplace_back(std::make_shared<FileLogSource>(
                file_log,
                storage_snapshot,
                modified_context,
                column_names,
                file_log.getMaxBlockSize(),
                file_log.getPollTimeoutMillisecond(),
                stream_number,
                max_streams_number,
                (*file_log.filelog_settings)[FileLogSetting::handle_error_mode],
                /* skip_broken_records */ false));
        }

        return Pipe::unitePipes(std::move(pipes));
    }

    const Names column_names;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
};

String resolveFileLogPath(const String & path, const String & user_files_path)
{
    if (user_files_path.empty() || std::filesystem::path(path).is_absolute() || fileOrSymlinkPathStartsWith(path, user_files_path))
        return path;
    return std::filesystem::path(user_files_path) / path;
}

StorageFileLog::StorageFileLog(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & path_,
    const String & metadata_base_path_,
    const String & format_name_,
    std::unique_ptr<FileLogSettings> settings,
    const String & comment,
    LoadingStrictnessLevel mode)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , filelog_context(configureContext(getContext()))
    , filelog_settings(std::move(settings))
    , path(resolveFileLogPath(path_, getContext()->getUserFilesPath()))
    , metadata_base_path(std::filesystem::path(metadata_base_path_) / "metadata")
    , format_name(format_name_)
    , log(getLogger("StorageFileLog (" + table_id_.getFullTableName() + ")"))
    , disk(getContext()->getStoragePolicy("default")->getDisks().at(0))
    , milliseconds_to_wait((*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals((*filelog_settings)[FileLogSetting::handle_error_mode]));
    setInMemoryMetadata(storage_metadata);

    if (!fileOrSymlinkPathStartsWith(path, getContext()->getUserFilesPath()))
    {
        if (LoadingStrictnessLevel::SECONDARY_CREATE <= mode)
        {
            LOG_ERROR(log, "The absolute data path should be inside `user_files_path`({})", getContext()->getUserFilesPath());
            return;
        }
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The absolute data path should be inside `user_files_path`({})", getContext()->getUserFilesPath());
    }

    bool created_metadata_directory = false;
    try
    {
        if (mode < LoadingStrictnessLevel::ATTACH)
        {
            if (disk->existsDirectory(metadata_base_path))
            {
                throw Exception(
                    ErrorCodes::TABLE_METADATA_ALREADY_EXISTS,
                    "Metadata files already exist by path: {}, remove them manually if it is intended",
                    metadata_base_path);
            }
            disk->createDirectories(metadata_base_path);
            created_metadata_directory = true;
        }

        loadMetaFiles(LoadingStrictnessLevel::ATTACH <= mode);
        loadFiles();

        chassert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
        chassert(file_infos.file_names.size() == file_infos.context_by_name.size());

        if (path_is_directory)
            directory_watch = std::make_unique<FileLogDirectoryWatcher>(root_data_path, *this, getContext());

        auto thread = getContext()->getSchedulePool()->createTask(getStorageID(), log->name(), [this] { threadFunc(); });
        task = std::make_shared<TaskContext>(std::move(thread));
    }
    catch (...)
    {
        if (mode <= LoadingStrictnessLevel::ATTACH)
        {
            if (created_metadata_directory)
                disk->removeRecursive(metadata_base_path);
            throw;
        }

        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

VirtualColumnsDescription StorageFileLog::createVirtuals(StreamingHandleErrorMode handle_error_mode)
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_filename", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_offset", std::make_shared<DataTypeUInt64>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    if (handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_record", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
        desc.addEphemeral("_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    }

    return desc;
}

void StorageFileLog::loadMetaFiles(bool attach)
{
    /// Attach table
    if (attach)
    {
        /// Meta file may lost, log and create directory
        if (!disk->existsDirectory(metadata_base_path))
        {
            /// Create metadata_base_path directory when store meta data
            LOG_ERROR(log, "Metadata files of table {} are lost.", getStorageID().getTableName());
        }
        /// Load all meta info to file_infos;
        deserialize();
    }
}

void StorageFileLog::loadFiles()
{
    auto absolute_path = std::filesystem::absolute(path);
    absolute_path = absolute_path.lexically_normal(); /// Normalize path.

    /// Files that the glob excludes but whose inode has a stored meta, with that inode.
    std::vector<std::pair<String, UInt64>> rotated_files;

    if (std::filesystem::is_regular_file(absolute_path))
    {
        path_is_directory = false;
        root_data_path = absolute_path.parent_path();

        file_infos.file_names.push_back(absolute_path.filename());
    }
    else
    {
        if (std::filesystem::is_directory(absolute_path))
        {
            root_data_path = absolute_path;
        }
        else
        {
            const String glob = absolute_path.filename();
            const auto directory = absolute_path.parent_path();
            if (containsGlobs(directory.string()) && !std::filesystem::is_directory(directory))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Globs are supported only in the file name of the path {}", absolute_path.c_str());
            if (!containsGlobs(glob))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The path {} neither a regular file, nor a directory", absolute_path.c_str());
            if (!std::filesystem::is_directory(directory))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The directory {} of the path {} does not exist", directory.c_str(), absolute_path.c_str());

            auto matcher = std::make_shared<re2::RE2>(makeRegexpPatternFromGlobs(glob));
            if (!matcher->ok())
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "Cannot compile regex from glob ({}): {}", glob, matcher->error());
            file_name_matcher = std::move(matcher);
            root_data_path = directory;
        }

        /// Just consider file with depth 1
        for (const auto & dir_entry : std::filesystem::directory_iterator{root_data_path})
        {
            if (!dir_entry.is_regular_file())
                continue;
            String file_name = dir_entry.path().filename();
            struct stat file_stat{};
            if (fileNameMatches(file_name))
                file_infos.file_names.push_back(std::move(file_name));
            else if (stat(dir_entry.path().c_str(), &file_stat) == 0 && file_infos.meta_by_inode.contains(file_stat.st_ino))
                rotated_files.emplace_back(std::move(file_name), file_stat.st_ino);
        }
    }

    /// Get files inode
    for (const auto & file : file_infos.file_names)
    {
        auto inode = getInode(getFullDataPath(file));
        file_infos.context_by_name.emplace(file, FileContext{.inode = inode});
    }

    /// A file renamed to a non-matching name while it was read (log rotation) keeps being read, under one of its names:
    /// the name it was read under, if that name still has it.
    std::ranges::sort(rotated_files);
    std::ranges::stable_partition(
        rotated_files, [this](const auto & rotated) { return file_infos.meta_by_inode.at(rotated.second).file_name == rotated.first; });
    for (auto & rotated : rotated_files)
    {
        const UInt64 inode = rotated.second;
        if (std::ranges::any_of(file_infos.context_by_name, [inode](const auto & file) { return file.second.inode == inode; }))
            continue;
        file_infos.context_by_name.emplace(rotated.first, FileContext{.inode = inode});
        file_infos.file_names.push_back(std::move(rotated.first));
    }

    /// Update file meta or create file meta
    std::vector<String> renamed_files;
    for (const auto & [file, ctx] : file_infos.context_by_name)
    {
        if (auto it = file_infos.meta_by_inode.find(ctx.inode); it != file_infos.meta_by_inode.end())
        {
            /// data file have been renamed, need update meta file's name
            if (it->second.file_name != file)
            {
                /// Through a temporary name: the renames of a rotation chain are visited in any order.
                if (disk->existsFile(getFullMetaPath(it->second.file_name)))
                    disk->replaceFile(getFullMetaPath(it->second.file_name), getFullMetaPath(file) + TMP_SUFFIX);
                it->second.file_name = file;
                renamed_files.push_back(file);
            }
        }
        /// New file
        else
        {
            FileMeta meta{file, 0, 0};
            file_infos.meta_by_inode.emplace(ctx.inode, meta);
        }
    }
    for (const auto & file : renamed_files)
        if (disk->existsFile(getFullMetaPath(file) + TMP_SUFFIX))
            disk->replaceFile(getFullMetaPath(file) + TMP_SUFFIX, getFullMetaPath(file));

    /// Clear unneeded meta file, because data files may be deleted
    if (file_infos.meta_by_inode.size() > file_infos.context_by_name.size())
    {
        InodeToFileMeta valid_metas;
        valid_metas.reserve(file_infos.context_by_name.size());
        for (const auto & [inode, meta] : file_infos.meta_by_inode)
        {
            /// Note, here we need to use inode to judge does the meta file is valid.
            /// In the case that when a file deleted, then we create new file with the
            /// same name, it will have different inode number with stored meta file,
            /// so the stored meta file is invalid
            if (auto it = file_infos.context_by_name.find(meta.file_name);
                it != file_infos.context_by_name.end() && it->second.inode == inode)
                valid_metas.emplace(inode, meta);
            /// Delete meta file from filesystem
            else
                disk->removeFileIfExists(getFullMetaPath(meta.file_name));
        }
        file_infos.meta_by_inode.swap(valid_metas);
    }
}

void StorageFileLog::serialize() const
{
    for (const auto & [inode, meta] : file_infos.meta_by_inode)
        serialize(inode, meta);
}

void StorageFileLog::serialize(UInt64 inode, const FileMeta & file_meta) const
{
    auto full_path = getFullMetaPath(file_meta.file_name);
    if (disk->existsFile(full_path))
    {
        checkOffsetIsValid(file_meta.file_name, file_meta.last_writen_position);
    }

    std::string tmp_path = full_path + TMP_SUFFIX;
    disk->removeFileIfExists(tmp_path);

    try
    {
        disk->createFile(tmp_path);
        auto out = disk->writeFile(tmp_path);
        writeIntText(inode, *out);
        writeChar('\n', *out);
        writeIntText(file_meta.last_writen_position, *out);
        out->finalize();
    }
    catch (...)
    {
        disk->removeFileIfExists(tmp_path);
        throw;
    }
    disk->replaceFile(tmp_path, full_path);
}

void StorageFileLog::deserialize()
{
    if (!disk->existsDirectory(metadata_base_path))
        return;

    std::vector<std::string> files_to_remove;

    /// In case of single file (not a watched directory),
    /// iterated directory always has one file inside.
    for (const auto dir_iter = disk->iterateDirectory(metadata_base_path); dir_iter->isValid(); dir_iter->next())
    {
        const auto & filename = dir_iter->name();
        if (filename.ends_with(TMP_SUFFIX))
        {
            files_to_remove.push_back(getFullMetaPath(filename));
            continue;
        }

        auto [metadata, inode] = readMetadata(filename);
        if (!metadata)
            continue;

        file_infos.meta_by_inode.emplace(inode, metadata);
    }

    for (const auto & file : files_to_remove)
        disk->removeFile(file);
}

UInt64 StorageFileLog::getInode(const String & file_name)
{
    struct stat file_stat{};
    if (stat(file_name.c_str(), &file_stat))
    {
        throw Exception(ErrorCodes::CANNOT_STAT, "Can not get stat info of file {}", file_name);
    }
    return file_stat.st_ino;
}

void StorageFileLog::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)

{
    query_plan.addStep(
        std::make_unique<ReadFromStorageFileLog>(column_names, shared_from_this(), storage_snapshot, query_info, std::move(query_context)));
}

void StorageFileLog::increaseStreams()
{
    running_streams += 1;
}

void StorageFileLog::reduceStreams()
{
    running_streams -= 1;
}

void StorageFileLog::drop()
{
    try
    {
        disk->removeRecursive(metadata_base_path);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageFileLog::startup()
{
    if (task)
        task->holder->activateAndSchedule();
}

void StorageFileLog::shutdown(bool)
{
    if (task)
    {
        task->stream_cancelled = true;

        /// Reader thread may wait for wake up
        wakeUp();

        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
        /// If no reading call and threadFunc, the log files will never
        /// be opened, also just leave the work of close files and
        /// store meta to streams. because if we close files in here,
        /// may result in data race with unfinishing reading pipeline
    }
}

void StorageFileLog::assertStreamGood(const std::ifstream & reader)
{
    if (!reader.good())
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Stream is in bad state");
    }
}

bool StorageFileLog::isTrackedByDirectoryEvents(const String & file_name) const
{
    return directory_watch && !FS::isSymlinkNoThrow(getFullDataPath(file_name));
}

void StorageFileLog::openFilesAndSetPos()
{
    bool any_open_failed = false;
    for (const auto & file : file_infos.file_names)
    {
        auto & file_ctx = findInMap(file_infos.context_by_name, file);
        if (file_ctx.status != FileStatus::NO_CHANGE || file_ctx.open_failed)
        {
            file_ctx.reader.emplace(getFullDataPath(file));
            const int open_errno = errno;
            if (!file_ctx.reader->is_open() && open_errno == ENOENT && isTrackedByDirectoryEvents(file))
            {
                /// Removed or renamed: the pending directory events drop the file or move its offset to the new name.
                file_ctx.reader.reset();
                file_ctx.status = FileStatus::NO_CHANGE;
                continue;
            }
            if (!file_ctx.reader->is_open()
                && (open_errno == ENOENT || open_errno == EACCES || open_errno == EPERM || open_errno == ELOOP))
            {
                if (!file_ctx.open_failed)
                    LOG_ERROR(log, "Cannot open file {}, will retry: {}", getFullDataPath(file), errnoToString(open_errno));
                file_ctx.reader.reset();
                file_ctx.status = FileStatus::NO_CHANGE;
                file_ctx.open_failed = true;
                /// The path leads to no file: what appears there later is read from its start, also after a restart.
                if (open_errno == ENOENT || open_errno == ELOOP)
                {
                    if (auto it = file_infos.meta_by_inode.find(file_ctx.inode);
                        it != file_infos.meta_by_inode.end() && it->second.file_name == file && it->second.last_writen_position != 0)
                    {
                        disk->removeFileIfExists(getFullMetaPath(file));
                        it->second.last_writen_position = 0;
                    }
                }
                any_open_failed = true;
                /// Published at once: a later file can throw before the end of the loop.
                has_files_to_reopen = true;
                continue;
            }
            auto & reader = file_ctx.reader.value();
            assertStreamGood(reader);
            if (file_ctx.open_failed)
            {
                /// The path may lead to another file now: read it from the start.
                if (const UInt64 inode = getInode(getFullDataPath(file)); inode != file_ctx.inode)
                {
                    if (isTrackedByDirectoryEvents(file))
                    {
                        file_ctx.reader.reset();
                        file_ctx.status = FileStatus::NO_CHANGE;
                        any_open_failed = true;
                        has_files_to_reopen = true;
                        continue;
                    }
                    file_infos.meta_by_inode.erase(file_ctx.inode);
                    disk->removeFileIfExists(getFullMetaPath(file));
                    file_ctx.inode = inode;
                    file_infos.meta_by_inode.insert_or_assign(inode, FileMeta{.file_name = file});
                }
                file_ctx.open_failed = false;
                file_ctx.status = FileStatus::UPDATED;
            }

            reader.seekg(0, std::ios::end);
            assertStreamGood(reader);

            auto file_end = reader.tellg();
            assertStreamGood(reader);

            auto & meta = findInMap(file_infos.meta_by_inode, file_ctx.inode);
            if (meta.last_writen_position > static_cast<UInt64>(file_end))
            {
                /// Truncated in place, e.g. by `logrotate` with `copytruncate`.
                LOG_INFO(
                    log,
                    "File {} is smaller than its saved offset ({} < {}), reading it again from the beginning",
                    file,
                    std::streamoff{file_end},
                    meta.last_writen_position);
                /// Before resetting: `serialize` refuses to store an offset smaller than the one on disk.
                disk->removeFileIfExists(getFullMetaPath(meta.file_name));
                meta.last_writen_position = 0;
            }
            /// update file end at the moment, used in ReadBuffer and serialize
            meta.last_open_end = file_end;

            reader.seekg(meta.last_writen_position);
            assertStreamGood(reader);
        }
    }
    has_files_to_reopen = any_open_failed;
    serialize();
}

void StorageFileLog::closeFilesAndStoreMeta(size_t start, size_t end)
{
    chassert(start < end);
    chassert(end <= file_infos.file_names.size());

    for (size_t i = start; i < end; ++i)
    {
        auto & file_ctx = findInMap(file_infos.context_by_name, file_infos.file_names[i]);

        if (file_ctx.reader)
        {
            if (file_ctx.reader->is_open())
                file_ctx.reader->close();
        }

        auto & meta = findInMap(file_infos.meta_by_inode, file_ctx.inode);
        serialize(file_ctx.inode, meta);
    }
}

void StorageFileLog::storeMetas(size_t start, size_t end)
{
    chassert(start < end);
    chassert(end <= file_infos.file_names.size());

    for (size_t i = start; i < end; ++i)
    {
        auto & file_ctx = findInMap(file_infos.context_by_name, file_infos.file_names[i]);

        auto & meta = findInMap(file_infos.meta_by_inode, file_ctx.inode);
        serialize(file_ctx.inode, meta);
    }
}

void StorageFileLog::checkOffsetIsValid(const String & filename, UInt64 offset) const
{
    auto [metadata, _] = readMetadata(filename);
    if (metadata.last_writen_position > offset)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Last stored last_written_position in meta file {} is bigger than current last_written_pos ({} > {})",
            filename, metadata.last_writen_position, offset);
    }
}

StorageFileLog::ReadMetadataResult StorageFileLog::readMetadata(const String & filename) const
{
    auto full_path = getFullMetaPath(filename);
    if (!disk->existsFile(full_path))
    {
        throw Exception(
            ErrorCodes::BAD_FILE_TYPE,
            "The file {} under {} is not a regular file",
            filename, metadata_base_path);
    }

    auto read_settings = getReadSettings();
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
    auto in = disk->readFile(full_path, read_settings);
    FileMeta metadata;
    UInt64 inode = 0;
    UInt64 last_written_pos = 0;

    if (in->eof()) /// File is empty.
    {
        disk->removeFile(full_path);
        return {};
    }

    if (!tryReadIntText(inode, *in))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed (1)", full_path);

    if (!checkChar('\n', *in))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed (2)", full_path);

    if (!tryReadIntText(last_written_pos, *in))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed (3)", full_path);

    metadata.file_name = filename;
    metadata.last_writen_position = last_written_pos;
    return { metadata, inode };
}

size_t StorageFileLog::getMaxBlockSize() const
{
    return (*filelog_settings)[FileLogSetting::max_block_size].changed ? (*filelog_settings)[FileLogSetting::max_block_size].value
                                                    : getContext()->getSettingsRef()[Setting::max_insert_block_size].value;
}

size_t StorageFileLog::getPollMaxBatchSize() const
{
    size_t batch_size = (*filelog_settings)[FileLogSetting::poll_max_batch_size].changed ? (*filelog_settings)[FileLogSetting::poll_max_batch_size].value
                                                                      : getContext()->getSettingsRef()[Setting::max_block_size].value;
    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageFileLog::getPollTimeoutMillisecond() const
{
    return (*filelog_settings)[FileLogSetting::poll_timeout_ms].changed ? (*filelog_settings)[FileLogSetting::poll_timeout_ms].totalMilliseconds()
                                                     : getContext()->getSettingsRef()[Setting::stream_poll_timeout_ms].totalMilliseconds();
}

bool StorageFileLog::checkDependencies(const StorageID & table_id)
{
    return !DatabaseCatalog::instance().getReadyDependentViews(table_id, getContext()).empty();
}

size_t StorageFileLog::getTableDependentCount() const
{
    auto table_id = getStorageID();
    // Check if at least one direct dependency is attached
    return DatabaseCatalog::instance().getDependentViews(table_id).size();
}

void StorageFileLog::threadFunc()
{
    bool reschedule = false;
    try
    {
        auto table_id = getStorageID();

        auto dependencies_count = getTableDependentCount();
        reschedule = !dependencies_count;

        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);
            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled)
            {
                if (!checkDependencies(table_id))
                {
                    /// For this case, we can not wait for watch thread to wake up
                    reschedule = true;
                    break;
                }

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                if (streamToViews())
                {
                    LOG_TRACE(log, "Stream stalled. Reschedule.");
                    if (milliseconds_to_wait
                        < static_cast<uint64_t>((*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds()))
                        milliseconds_to_wait *= (*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value;
                    break;
                }

                milliseconds_to_wait = (*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();


                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    reschedule = true;
                    break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    mv_attached.store(false);

    // Wait for attached views
    if (!task->stream_cancelled)
    {
        if (path_is_directory)
        {
            if (!getTableDependentCount() || reschedule || has_files_to_reopen)
                task->holder->scheduleAfter(milliseconds_to_wait);
            else
            {
                std::unique_lock<std::mutex> lock(mutex);
                /// Waiting for watch directory thread to wake up
                cv.wait(lock, [this] { return has_new_events; });
                has_new_events = false;

                if (task->stream_cancelled)
                    return;
                task->holder->schedule();
            }
        }
        else
            task->holder->scheduleAfter(milliseconds_to_wait);
    }
}

bool StorageFileLog::streamToViews()
{
    std::lock_guard lock(file_infos_mutex);
    if (running_streams)
    {
        LOG_INFO(log, "Another select query is running on this table, need to wait it finish.");
        return true;
    }

    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist", table_id.getNameForLogs());

    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    auto storage_snapshot = getStorageSnapshot(metadata_snapshot, getContext());

    auto max_streams_number = std::min<UInt64>((*filelog_settings)[FileLogSetting::max_threads].value, file_infos.file_names.size());
    /// No files to parse
    if (max_streams_number == 0)
    {
        LOG_INFO(log, "There is a idle table named {}, no files need to parse.", getName());
        return updateFileInfos();
    }

    /// Nothing to read until the watcher events are applied, e.g. when only files that the glob excludes changed.
    /// A file that could not be opened is retried by `openFilesAndSetPos`.
    if (std::ranges::all_of(
            file_infos.context_by_name,
            [](const auto & file) { return file.second.status == FileStatus::NO_CHANGE && !file.second.open_failed; }))
        return updateFileInfos();

    // Create an INSERT query for streaming data
    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = table_id;

    auto new_context = Context::createCopy(filelog_context);

    /// Create a fresh query context from filelog_context, discarding any caches attached to the previous context to
    /// ensure no stale state is reused.
    new_context->makeQueryContext();

    InterpreterInsertQuery interpreter(
        insert,
        new_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_isnert */ false);

    auto block_io = interpreter.execute();

    read_more_after_skipped_records = false;
    /// Each stream responsible for closing it's files and store meta
    openFilesAndSetPos();

    Pipes pipes;
    pipes.reserve(max_streams_number);
    for (size_t stream_number = 0; stream_number < max_streams_number; ++stream_number)
    {
        pipes.emplace_back(std::make_shared<FileLogSource>(
            *this,
            storage_snapshot,
            new_context,
            block_io.pipeline.getHeader().getNames(),
            getPollMaxBatchSize(),
            getPollTimeoutMillisecond(),
            stream_number,
            max_streams_number,
            (*filelog_settings)[FileLogSetting::handle_error_mode],
            /* skip_broken_records */ true));
    }

    auto input= Pipe::unitePipes(std::move(pipes));

    assertBlocksHaveEqualStructure(input.getHeader(), block_io.pipeline.getHeader(), "StorageFileLog streamToViews");

    std::atomic<size_t> rows = 0;
    {
        block_io.pipeline.complete(std::move(input));
        block_io.pipeline.setNumThreads(max_streams_number);
        block_io.pipeline.setConcurrencyControl(new_context->getSettingsRef()[Setting::use_concurrency_control]);
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.", rows.load(), table_id.getNameForLogs(), milliseconds);

    bool stalled = updateFileInfos();
    return stalled && !read_more_after_skipped_records;
}

void StorageFileLog::wakeUp()
{
    std::unique_lock<std::mutex> lock(mutex);
    has_new_events = true;
    lock.unlock();
    cv.notify_one();
}

void registerStorageFileLog(StorageFactory & factory);
void registerStorageFileLog(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();

        bool has_settings = args.storage_def->settings;

        auto filelog_settings = std::make_unique<FileLogSettings>();
        if (has_settings)
        {
            filelog_settings->loadFromQuery(*args.storage_def);
        }

        auto cpu_cores = getNumberOfCPUCoresToUse();
        auto num_threads = (*filelog_settings)[FileLogSetting::max_threads];

        if ((*filelog_settings)[FileLogSetting::max_threads].is_auto) /// Default
        {
            num_threads = std::max(1U, cpu_cores / 4);
            (*filelog_settings)[FileLogSetting::max_threads] = num_threads;
        }
        else if (num_threads > cpu_cores)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of threads to parse files can not be bigger than {}", cpu_cores);
        }
        else if (num_threads < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of threads to parse files can not be lower than 1");
        }

        if ((*filelog_settings)[FileLogSetting::max_block_size].changed && (*filelog_settings)[FileLogSetting::max_block_size].value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "filelog_max_block_size can not be lower than 1");
        }

        if ((*filelog_settings)[FileLogSetting::poll_max_batch_size].changed && (*filelog_settings)[FileLogSetting::poll_max_batch_size].value < 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "filelog_poll_max_batch_size can not be lower than 1");
        }

        size_t init_sleep_time = (*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();
        size_t max_sleep_time = (*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds();
        if (init_sleep_time > max_sleep_time)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "poll_directory_watch_events_backoff_init can not "
                            "be greater than poll_directory_watch_events_backoff_max");
        }

        if ((*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].changed
            && !(*filelog_settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "poll_directory_watch_events_backoff_factor can not be 0");

        if ((*filelog_settings)[FileLogSetting::handle_error_mode].changed && (*filelog_settings)[FileLogSetting::handle_error_mode].value == StreamingHandleErrorMode::DEAD_LETTER_QUEUE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DEAD_LETTER_QUEUE is not supported by the table engine");

        if (args_count != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of StorageFileLog should be 2, path and format name");

        auto path_ast = evaluateConstantExpressionAsLiteral(engine_args[0], args.getContext());
        auto format_ast = evaluateConstantExpressionAsLiteral(engine_args[1], args.getContext());

        auto path = checkAndGetLiteralArgument<String>(path_ast, "path");
        auto format = checkAndGetLiteralArgument<String>(format_ast, "format");

        return std::make_shared<StorageFileLog>(
            args.table_id,
            args.getContext(),
            args.columns,
            path,
            args.relative_data_path,
            format,
            std::move(filelog_settings),
            args.comment,
            args.mode);
    };

    factory.registerStorage(
        "FileLog",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .has_builtin_setting_fn = FileLogSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
This engine allows processing of application log files as a stream of records.

`FileLog` lets you:

- Subscribe to log files.
- Process new records as they are appended to subscribed log files.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

Engine arguments:

- `path_to_logs` – Path to log files to subscribe. It can be path to a directory with log files or to a single log file. A relative path is resolved against the `user_files_path` directory, like in the [file](/reference/functions/table-functions/file) table function; a relative path that is already inside `user_files_path` from the working directory of the server (for example, `user_files/my_app/app.log` when the server runs in its data directory, as in the official Docker image) keeps that meaning. The file name in the path can have [globs](/reference/functions/table-functions/file#globs-in-path) (`*`, `?`, `{abc,def}`, `{N..M}`) to read only the matching files of the directory, see [Selecting files with globs](#selecting-files-with-globs). Note that ClickHouse allows only paths inside `user_files` directory.
- `format_name` - Record format. Note that FileLog process each line in a file as a separate record and not all data formats are suitable for it.

Optional parameters:

- `poll_timeout_ms` - Timeout for single poll from log file. Default: [stream_poll_timeout_ms](/reference/settings/session-settings/stream#stream_poll_timeout_ms).
- `poll_max_batch_size` — Maximum amount of records to be polled in a single poll. Default: [max_block_size](/reference/settings/session-settings/max#max_block_size).
- `max_block_size` — The maximum batch size (in records) for poll. Default: [max_insert_block_size](/reference/settings/session-settings/max-insert#max_insert_block_size).
- `max_threads` - Number of max threads to parse files, default is 0, which means the number will be max(1, physical_cpu_cores / 4).
- `poll_directory_watch_events_backoff_init` - The initial sleep value for watch directory thread. Default: `500`.
- `poll_directory_watch_events_backoff_max` - The max sleep value for watch directory thread. Default: `32000`.
- `poll_directory_watch_events_backoff_factor` - The speed of backoff, exponential by default. Default: `2`.
- `handle_error_mode` — How to handle errors for FileLog engine. Possible values: default (a direct `SELECT` throws an exception if a record fails to parse; while the table streams into materialized views, such a record is skipped and the error is written to the server log), stream (the exception message and raw record will be saved in virtual columns `_error` and `_raw_record`).

## Description {#description}

The delivered records are tracked automatically, so each record in a log file is only counted once. A file that is shorter than the offset recorded for it when it is next read, as after `logrotate` with `copytruncate`, is read again from the beginning. A truncation is not detected if the file grows back to at least that offset before it is next read.

A file that cannot be opened (a symlink whose target was removed, a file not readable by the server, or the file of a single-file table that was removed) is skipped with an error in the server log and retried until it can be opened while the table is loaded; a file that was missing is then read from its start. A symlink is read only if its target exists when the table finds it.

`SELECT` is not particularly useful for reading records (except for debugging), because each record can be read only once. It is more practical to create real-time threads using [materialized views](/reference/statements/create/view). To do this:

1.  Use the engine to create a FileLog table and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive records from log files and convert them to the required format using `SELECT`.
One FileLog table can have as many materialized views as you like, they do not read data from the table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

```sql
CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('my_app/app.log', 'JSONEachRow');

CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree
  PARTITION BY toYYYYMM(day)
  ORDER BY (day, level);

CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM logs GROUP BY day, level;

SELECT level, sum(total) FROM daily GROUP BY level;
```

To stop receiving streams data or to change the conversion logic, detach the materialized view:

```sql
DETACH TABLE consumer;
ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Selecting files with globs {#selecting-files-with-globs}

When the file name in `path_to_logs` has globs, the table reads the files of that directory whose names match, including the ones that appear later. Globs are not supported in the directory part of the path.

A file that the table reads keeps being read when it is renamed to a name that does not match, until it is removed from the directory. A file that is created and renamed to a name that does not match while the table is detached or the server is stopped is not read. This is what log rotation needs. For example, `logrotate` with `compress` and `delaycompress` keeps the directory like this:

```text
app.log         the file the application writes
app.log.1       the previous file, renamed by logrotate, compressed on the next rotation
app.log.2.gz    older files, compressed
```

A table on `FileLog('/var/lib/clickhouse/user_files/my_app/*.log', 'JSONEachRow')` reads `app.log`; after the rotation it keeps reading `app.log.1`, so the lines the application writes there before it reopens its log are not lost, and it never reads the compressed files. Make sure the glob does not match the compressed file names.

## Virtual columns {#virtual-columns}

- `_filename` - Name of the log file. Data type: `LowCardinality(String)`.
- `_offset` - Offset in the log file. Data type: `UInt64`.

Additional virtual columns when `handle_error_mode='stream'`:

- `_raw_record` - Raw record that couldn't be parsed successfully. Data type: `Nullable(String)`.
- `_error` - Exception message happened during failed parsing. Data type: `Nullable(String)`.

Note: `_raw_record` and `_error` virtual columns are filled only in case of exception during parsing, they are always `NULL` when message was parsed successfully.

## Data durability {#data-durability}

The `FileLog` engine records the offset it has consumed for a chunk before the insert that chunk belongs to has been committed, so an interrupted server can leave the recorded offset ahead of the data that reached the target table. On restart each log file resumes from the offset recorded in its metadata directory, so those rows are never re-read: they are lost with no error and `count()` is simply smaller. An ordinary process failure is enough to expose this, and it does not require a power loss, because the offset is recorded in a metadata file that is renamed into place while the target part is still being written.

A loss of the OS page cache can additionally discard data that had already been written to the target table; examples are a device-level power loss and an unclean host or kernel reset. The metadata files holding the offsets are themselves written without an fsync of the file or of its directory, so they carry no durability guarantee of their own either.

Unlike the message-broker engines, `FileLog` cannot be protected against this by making the target durable first. Because the offset is recorded from inside the reading pipeline, before the insert it belongs to has finished, setting `fsync_after_insert = 1` on the target `MergeTree` tables does not establish the inserted part as durable before the offset advances. Treat `FileLog` consumption as best-effort tailing of local files: where no rows may be lost, keep the source log files until the consumed data has been verified in the target, so that consumption can be repeated. Dropping and recreating the table discards the recorded offsets and re-reads the files from the beginning.
)DOCS_MD",
            .syntax = "ENGINE = FileLog('path_to_logs', 'format') SETTINGS ...",
            .related = {"Kafka", "RabbitMQ", "NATS"}});
}

void StorageFileLog::onFileAppeared(const String & file_name, UInt64 inode)
{
    auto it = file_infos.context_by_name.find(file_name);
    if (it == file_infos.context_by_name.end())
    {
        file_infos.file_names.push_back(file_name);
        file_infos.context_by_name.emplace(file_name, FileContext{.inode = inode});
        return;
    }
    if (it->second.inode != inode)
    {
        if (auto meta = file_infos.meta_by_inode.find(it->second.inode);
            meta != file_infos.meta_by_inode.end() && meta->second.file_name == file_name)
        {
            file_infos.meta_by_inode.erase(meta);
            disk->removeFileIfExists(getFullMetaPath(file_name));
        }
    }
    it->second = FileContext{.inode = inode};
}

bool StorageFileLog::fileNameMatches(const String & file_name) const
{
    return !file_name_matcher || re2::RE2::FullMatch(file_name, *file_name_matcher);
}

bool StorageFileLog::updateFileInfos()
{
    if (!directory_watch)
    {
        if (file_infos.file_names.empty())
            return false;

        /// For table just watch one file, we can not use directory monitor to watch it
        if (!path_is_directory)
        {
            chassert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
            chassert(file_infos.file_names.size() == file_infos.context_by_name.size());
            chassert(file_infos.file_names.size() == 1);

            if (auto it = file_infos.context_by_name.find(file_infos.file_names[0]); it != file_infos.context_by_name.end())
            {
                it->second.status = FileStatus::UPDATED;
                return true;
            }
        }
        return false;
    }

    /// We process directory watcher events even when `file_names` is empty. After
    /// the only watched file is removed the cleanup loop empties `file_names`;
    /// without consuming the watcher's queue here we would miss a subsequent
    /// `DW_ITEM_ADDED`/`DW_ITEM_MOVED_TO` and never observe the recreated file.

    /// Do not need to hold file_status lock, since it will be holded
    /// by caller when call this function
    auto error = directory_watch->getErrorAndReset();
    if (error.has_error)
        LOG_ERROR(log, "Error happened during watching directory {}: {}", directory_watch->getPath(), error.error_msg);

    /// These file infos should always have same size(one for one) before update and after update
    chassert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
    chassert(file_infos.file_names.size() == file_infos.context_by_name.size());

    auto events = directory_watch->getEventsAndReset();

    /// Walk events in the order the kernel emitted them. Rename pairs
    /// (`DW_ITEM_MOVED_FROM` on one name + `DW_ITEM_MOVED_TO` on another) must
    /// be observed before any later `DW_ITEM_ADDED` for the source name, so
    /// that `onFileAppeared`'s filename-ownership guard sees the post-rename
    /// `file_name` in `meta_by_inode` rather than the stale pre-rename one.
    /// Only the last add, removal or rename of a name is applied to the file found there now; earlier events of the
    /// name only record whether the file they leave under it is read.
    std::unordered_map<String, size_t> last_change;
    for (size_t i = 0; i < events.size(); ++i)
        if (events[i].second.type != DirectoryWatcherBase::DW_ITEM_MODIFIED)
            last_change[events[i].first] = i;
    std::unordered_map<String, bool> name_is_read;
    std::unordered_set<UInt64> renamed_from_read_name;
    auto is_read = [&](const String & name)
    {
        auto it = name_is_read.find(name);
        return it != name_is_read.end() ? it->second : fileNameMatches(name) || file_infos.context_by_name.contains(name);
    };

    for (size_t i = 0; i < events.size(); ++i)
    {
        const auto & [file_name, event_info] = events[i];
        String file_path = getFullDataPath(file_name);
        LOG_TRACE(log, "New event {} watched, file_name: {}", event_info.callback, file_name);

        switch (event_info.type)
        {
            case DirectoryWatcherBase::DW_ITEM_ADDED:
            {
                name_is_read[file_name] = fileNameMatches(file_name);
                if (last_change.at(file_name) != i)
                    break;
                /// Check if it is a regular file, and new file may be renamed or removed
                if (std::filesystem::is_regular_file(file_path) && fileNameMatches(file_name))
                {
                    auto inode = getInode(file_path);

                    onFileAppeared(file_name, inode);

                    /// An added file is read from offset 0, so any on-disk meta
                    /// under this name is stale. Drop it to stay consistent with
                    /// the pos-0 meta set below, else serialize() sees a larger
                    /// stored offset and raises LOGICAL_ERROR.
                    disk->removeFileIfExists(getFullMetaPath(file_name));

                    if (auto it = file_infos.meta_by_inode.find(inode); it != file_infos.meta_by_inode.end())
                        it->second = FileMeta{.file_name = file_name};
                    else
                        file_infos.meta_by_inode.emplace(inode, FileMeta{.file_name = file_name});
                }
                break;
            }

            case DirectoryWatcherBase::DW_ITEM_MODIFIED:
            {
                /// When new file added and appended, it has two event: DW_ITEM_ADDED
                /// and DW_ITEM_MODIFIED, since the order of these two events in the
                /// sequence is uncentain, so we may can not find it in file_infos, just
                /// skip it, the file info will be handled in DW_ITEM_ADDED case.
                if (auto it = file_infos.context_by_name.find(file_name);
                    it != file_infos.context_by_name.end() && it->second.status != FileStatus::REMOVED)
                    it->second.status = FileStatus::UPDATED;
                break;
            }

            case DirectoryWatcherBase::DW_ITEM_REMOVED:
            /// The file **left** the directory
            case DirectoryWatcherBase::DW_ITEM_MOVED_FROM:
            {
                if (event_info.type == DirectoryWatcherBase::DW_ITEM_MOVED_FROM && event_info.cookie && is_read(file_name))
                    renamed_from_read_name.insert(event_info.cookie);
                name_is_read[file_name] = false;
                if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                    it->second.status = FileStatus::REMOVED;
                break;
            }
            /// The file **arrived** in this directory
            case DirectoryWatcherBase::DW_ITEM_MOVED_TO:
            {
                const bool renamed_from_read = event_info.cookie && renamed_from_read_name.contains(event_info.cookie);
                name_is_read[file_name] = renamed_from_read || fileNameMatches(file_name);
                if (last_change.at(file_name) != i)
                    break;
                /// Similar to DW_ITEM_ADDED, but if it removed from an old file
                /// should obtain old meta file and rename meta file
                if (std::filesystem::is_regular_file(file_path))
                {
                    auto inode = getInode(file_path);

                    /// Another name of a file that is still read here (a hard link) is not read again.
                    const bool read_under_other_name = std::ranges::any_of(
                        file_infos.context_by_name,
                        [&](const auto & file)
                        { return file.second.inode == inode && file.second.status != FileStatus::REMOVED && file.first != file_name; });
                    if (!fileNameMatches(file_name) && ((!file_infos.meta_by_inode.contains(inode) && !renamed_from_read) || read_under_other_name))
                    {
                        /// The file read under this name, if any, was replaced by one that is not read.
                        if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                            it->second.status = FileStatus::REMOVED;
                        break;
                    }

                    onFileAppeared(file_name, inode);

                    /// File has been renamed, we should also rename meta file
                    if (auto it = file_infos.meta_by_inode.find(inode); it != file_infos.meta_by_inode.end())
                    {
                        auto old_name = it->second.file_name;
                        it->second.file_name = file_name;
                        /// Meta paths are relative to the disk root, so go through the disk abstraction:
                        /// std::filesystem would resolve them against the process CWD and silently skip
                        /// the rename, leaving a stale meta file that collides when the name is reused
                        /// (e.g. a logrotate rename chain processed in one batch).
                        if (disk->existsFile(getFullMetaPath(old_name)))
                            disk->replaceFile(getFullMetaPath(old_name), getFullMetaPath(file_name));
                    }
                    /// The rename source was not tracked, e.g. it was created and renamed within one batch
                    else
                        file_infos.meta_by_inode.emplace(inode, FileMeta{.file_name = file_name});
                }
                break;
            }
        }
    }
    std::vector<String> valid_files;

    /// Remove file infos with REMOVE status
    for (const auto & file_name : file_infos.file_names)
    {
        if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
        {
            if (it->second.status == FileStatus::REMOVED)
            {
                /// We need to check that this inode does not hold by other file(mv),
                /// otherwise, we can not destroy it.
                auto inode = it->second.inode;
                /// If it's now hold by other file, than the file_name should has
                /// been changed during updating file_infos
                if (auto meta = file_infos.meta_by_inode.find(inode);
                    meta != file_infos.meta_by_inode.end() && meta->second.file_name == file_name)
                    file_infos.meta_by_inode.erase(meta);

                disk->removeFileIfExists(getFullMetaPath(file_name));
                file_infos.context_by_name.erase(it);
            }
            else
            {
                valid_files.push_back(file_name);
            }
        }
    }
    file_infos.file_names.swap(valid_files);

    /// These file infos should always have same size(one for one)
    chassert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
    chassert(file_infos.file_names.size() == file_infos.context_by_name.size());

    return events.empty() || file_infos.file_names.empty();
}

}
