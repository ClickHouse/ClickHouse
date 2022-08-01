#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteIntText.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/FileLog/FileLogSource.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/filesystemHelpers.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

#include <sys/stat.h>

namespace DB
{

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
}

namespace
{
    const auto MAX_THREAD_WORK_DURATION_MS = 60000;
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
    bool attach)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , filelog_settings(std::move(settings))
    , path(path_)
    , metadata_base_path(std::filesystem::path(metadata_base_path_) / "metadata")
    , format_name(format_name_)
    , log(&Poco::Logger::get("StorageFileLog (" + table_id_.table_name + ")"))
    , milliseconds_to_wait(filelog_settings->poll_directory_watch_events_backoff_init.totalMilliseconds())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    try
    {
        loadMetaFiles(attach);
        loadFiles();

        assert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
        assert(file_infos.file_names.size() == file_infos.context_by_name.size());

        if (path_is_directory)
            directory_watch = std::make_unique<FileLogDirectoryWatcher>(root_data_path, *this, getContext());

        auto thread = getContext()->getSchedulePool().createTask(log->name(), [this] { threadFunc(); });
        task = std::make_shared<TaskContext>(std::move(thread));
    }
    catch (...)
    {
        if (!attach)
            throw;
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageFileLog::loadMetaFiles(bool attach)
{
    /// Attach table
    if (attach)
    {
        const auto & storage = getStorageID();

        auto metadata_path_exist = std::filesystem::exists(metadata_base_path);
        auto previous_path = std::filesystem::path(getContext()->getPath()) / ".filelog_storage_metadata" / storage.getDatabaseName() / storage.getTableName();

        /// For compatibility with the previous path version.
        if (std::filesystem::exists(previous_path) && !metadata_path_exist)
        {
            std::filesystem::copy(previous_path, metadata_base_path, std::filesystem::copy_options::recursive);
            std::filesystem::remove_all(previous_path);
        }
        /// Meta file may lost, log and create directory
        else if (!metadata_path_exist)
        {
            /// Create metadata_base_path directory when store meta data
            LOG_ERROR(log, "Metadata files of table {} are lost.", getStorageID().getTableName());
        }
        /// Load all meta info to file_infos;
        deserialize();
    }
    /// Create table, just create meta data directory
    else
    {
        if (std::filesystem::exists(metadata_base_path))
        {
            throw Exception(
                ErrorCodes::TABLE_METADATA_ALREADY_EXISTS,
                "Metadata files already exist by path: {}, remove them manually if it is intended",
                metadata_base_path);
        }
        /// We do not create the metadata_base_path directory at creation time, create it at the moment of serializing
        /// meta files, such that can avoid unnecessarily create this directory if create table failed.
    }
}

void StorageFileLog::loadFiles()
{
    if (!fileOrSymlinkPathStartsWith(path, getContext()->getUserFilesPath()))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The absolute data path should be inside `user_files_path`({})", getContext()->getUserFilesPath());
    }

    auto absolute_path = std::filesystem::absolute(path);
    absolute_path = absolute_path.lexically_normal(); /// Normalize path.

    if (std::filesystem::is_regular_file(absolute_path))
    {
        path_is_directory = false;
        root_data_path = absolute_path.parent_path();

        file_infos.file_names.push_back(absolute_path.filename());
    }
    else if (std::filesystem::is_directory(absolute_path))
    {
        root_data_path = absolute_path;
        /// Just consider file with depth 1
        for (const auto & dir_entry : std::filesystem::directory_iterator{absolute_path})
        {
            if (dir_entry.is_regular_file())
            {
                file_infos.file_names.push_back(dir_entry.path().filename());
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The path {} neither a regular file, nor a directory", absolute_path.c_str());
    }

    /// Get files inode
    for (const auto & file : file_infos.file_names)
    {
        auto inode = getInode(getFullDataPath(file));
        file_infos.context_by_name.emplace(file, FileContext{.inode = inode});
    }

    /// Update file meta or create file meta
    for (const auto & [file, ctx] : file_infos.context_by_name)
    {
        if (auto it = file_infos.meta_by_inode.find(ctx.inode); it != file_infos.meta_by_inode.end())
        {
            /// data file have been renamed, need update meta file's name
            if (it->second.file_name != file)
            {
                std::filesystem::rename(getFullMetaPath(it->second.file_name), getFullMetaPath(file));
                it->second.file_name = file;
            }
        }
        /// New file
        else
        {
            FileMeta meta{file, 0, 0};
            file_infos.meta_by_inode.emplace(ctx.inode, meta);
        }
    }

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
                std::filesystem::remove(getFullMetaPath(meta.file_name));
        }
        file_infos.meta_by_inode.swap(valid_metas);
    }
}

void StorageFileLog::serialize() const
{
    if (!std::filesystem::exists(metadata_base_path))
    {
        std::filesystem::create_directories(metadata_base_path);
    }
    for (const auto & [inode, meta] : file_infos.meta_by_inode)
    {
        auto full_name = getFullMetaPath(meta.file_name);
        if (!std::filesystem::exists(full_name))
        {
            FS::createFile(full_name);
        }
        else
        {
            checkOffsetIsValid(full_name, meta.last_writen_position);
        }
        WriteBufferFromFile out(full_name);
        writeIntText(inode, out);
        writeChar('\n', out);
        writeIntText(meta.last_writen_position, out);
        out.finalize();
    }
}

void StorageFileLog::serialize(UInt64 inode, const FileMeta & file_meta) const
{
    if (!std::filesystem::exists(metadata_base_path))
    {
        std::filesystem::create_directories(metadata_base_path);
    }
    auto full_name = getFullMetaPath(file_meta.file_name);
    if (!std::filesystem::exists(full_name))
    {
        FS::createFile(full_name);
    }
    else
    {
        checkOffsetIsValid(full_name, file_meta.last_writen_position);
    }
    WriteBufferFromFile out(full_name);
    writeIntText(inode, out);
    writeChar('\n', out);
    writeIntText(file_meta.last_writen_position, out);
    out.finalize();
}

void StorageFileLog::deserialize()
{
    if (!std::filesystem::exists(metadata_base_path))
        return;
    /// In case of single file (not a watched directory),
    /// iterated directory always has one file inside.
    for (const auto & dir_entry : std::filesystem::directory_iterator{metadata_base_path})
    {
        if (!dir_entry.is_regular_file())
        {
            throw Exception(
                ErrorCodes::BAD_FILE_TYPE,
                "The file {} under {} is not a regular file when deserializing meta files",
                dir_entry.path().c_str(),
                metadata_base_path);
        }

        ReadBufferFromFile in(dir_entry.path().c_str());
        FileMeta meta;
        UInt64 inode, last_written_pos;

        if (!tryReadIntText(inode, in))
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed", dir_entry.path().c_str());
        }
        assertChar('\n', in);
        if (!tryReadIntText(last_written_pos, in))
        {
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed", dir_entry.path().c_str());
        }

        meta.file_name = dir_entry.path().filename();
        meta.last_writen_position = last_written_pos;

        file_infos.meta_by_inode.emplace(inode, meta);
    }
}

UInt64 StorageFileLog::getInode(const String & file_name)
{
    struct stat file_stat;
    if (stat(file_name.c_str(), &file_stat))
    {
        throw Exception(ErrorCodes::CANNOT_STAT, "Can not get stat info of file {}", file_name);
    }
    return file_stat.st_ino;
}

Pipe StorageFileLog::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    /// If there are MVs depended on this table, we just forbid reading
    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageFileLog with attached materialized views");

    std::lock_guard<std::mutex> lock(file_infos_mutex);
    if (running_streams)
    {
        throw Exception("Another select query is running on this table, need to wait it finish.", ErrorCodes::CANNOT_SELECT);
    }

    updateFileInfos();

    /// No files to parse
    if (file_infos.file_names.empty())
    {
        LOG_WARNING(log, "There is a idle table named {}, no files need to parse.", getName());
        return Pipe{};
    }

    auto modified_context = Context::createCopy(local_context);

    auto max_streams_number = std::min<UInt64>(filelog_settings->max_threads, file_infos.file_names.size());

    /// Each stream responsible for closing it's files and store meta
    openFilesAndSetPos();

    Pipes pipes;
    pipes.reserve(max_streams_number);
    for (size_t stream_number = 0; stream_number < max_streams_number; ++stream_number)
    {
        pipes.emplace_back(std::make_shared<FileLogSource>(
            *this,
            storage_snapshot,
            modified_context,
            column_names,
            getMaxBlockSize(),
            getPollTimeoutMillisecond(),
            stream_number,
            max_streams_number));
    }

    return Pipe::unitePipes(std::move(pipes));
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
        if (std::filesystem::exists(metadata_base_path))
            std::filesystem::remove_all(metadata_base_path);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageFileLog::startup()
{
    try
    {
        if (task)
        {
            task->holder->activateAndSchedule();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageFileLog::shutdown()
{
    try
    {
        if (task)
        {
            task->stream_cancelled = true;

            /// Reader thread may wait for wake up
            wakeUp();

            LOG_TRACE(log, "Waiting for cleanup");
            task->holder->deactivate();
        }
        /// If no reading call and threadFunc, the log files will never
        /// be opened, also just leave the work of close files and
        /// store meta to streams. because if we close files in here,
        /// may result in data race with unfinishing reading pipeline
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        task->holder->deactivate();
    }
}

void StorageFileLog::assertStreamGood(const std::ifstream & reader)
{
    if (!reader.good())
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Stream is in bad state");
    }
}

void StorageFileLog::openFilesAndSetPos()
{
    for (const auto & file : file_infos.file_names)
    {
        auto & file_ctx = findInMap(file_infos.context_by_name, file);
        if (file_ctx.status != FileStatus::NO_CHANGE)
        {
            file_ctx.reader.emplace(getFullDataPath(file));
            auto & reader = file_ctx.reader.value();
            assertStreamGood(reader);

            reader.seekg(0, reader.end);
            assertStreamGood(reader);

            auto file_end = reader.tellg();
            assertStreamGood(reader);

            auto & meta = findInMap(file_infos.meta_by_inode, file_ctx.inode);
            if (meta.last_writen_position > static_cast<UInt64>(file_end))
            {
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Last saved offsset for File {} is bigger than file size ({} > {})",
                    file,
                    meta.last_writen_position,
                    file_end);
            }
            /// update file end at the moment, used in ReadBuffer and serialize
            meta.last_open_end = file_end;

            reader.seekg(meta.last_writen_position);
            assertStreamGood(reader);
        }
    }
    serialize();
}

void StorageFileLog::closeFilesAndStoreMeta(size_t start, size_t end)
{
    assert(start < end);
    assert(end <= file_infos.file_names.size());

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
    assert(start < end);
    assert(end <= file_infos.file_names.size());

    for (size_t i = start; i < end; ++i)
    {
        auto & file_ctx = findInMap(file_infos.context_by_name, file_infos.file_names[i]);

        auto & meta = findInMap(file_infos.meta_by_inode, file_ctx.inode);
        serialize(file_ctx.inode, meta);
    }
}

void StorageFileLog::checkOffsetIsValid(const String & full_name, UInt64 offset)
{
    ReadBufferFromFile in(full_name);
    UInt64 _, last_written_pos;

    if (!tryReadIntText(_, in))
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed", full_name);
    }
    assertChar('\n', in);
    if (!tryReadIntText(last_written_pos, in))
    {
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read meta file {} failed", full_name);
    }
    if (last_written_pos > offset)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Last stored last_written_pos in meta file {} is bigger than current last_written_pos", full_name);
}

size_t StorageFileLog::getMaxBlockSize() const
{
    return filelog_settings->max_block_size.changed ? filelog_settings->max_block_size.value
                                                    : getContext()->getSettingsRef().max_insert_block_size.value;
}

size_t StorageFileLog::getPollMaxBatchSize() const
{
    size_t batch_size = filelog_settings->poll_max_batch_size.changed ? filelog_settings->poll_max_batch_size.value
                                                                      : getContext()->getSettingsRef().max_block_size.value;
    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageFileLog::getPollTimeoutMillisecond() const
{
    return filelog_settings->poll_timeout_ms.changed ? filelog_settings->poll_timeout_ms.totalMilliseconds()
                                                     : getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
}

bool StorageFileLog::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    for (const auto & storage : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(storage, getContext());
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(storage))
            return false;
    }

    return true;
}

size_t StorageFileLog::getTableDependentCount() const
{
    auto table_id = getStorageID();
    // Check if at least one direct dependency is attached
    return DatabaseCatalog::instance().getDependencies(table_id).size();
}

void StorageFileLog::threadFunc()
{
    bool reschedule = false;
    try
    {
        auto table_id = getStorageID();

        auto dependencies_count = getTableDependentCount();

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
                        < static_cast<uint64_t>(filelog_settings->poll_directory_watch_events_backoff_max.totalMilliseconds()))
                        milliseconds_to_wait *= filelog_settings->poll_directory_watch_events_backoff_factor.value;
                    break;
                }
                else
                {
                    milliseconds_to_wait = filelog_settings->poll_directory_watch_events_backoff_init.totalMilliseconds();
                }

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
            if (!getTableDependentCount() || reschedule)
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
    std::lock_guard<std::mutex> lock(file_infos_mutex);
    if (running_streams)
    {
        LOG_INFO(log, "Another select query is running on this table, need to wait it finish.");
        return true;
    }

    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_snapshot = getStorageSnapshot(metadata_snapshot, getContext());

    auto max_streams_number = std::min<UInt64>(filelog_settings->max_threads.value, file_infos.file_names.size());
    /// No files to parse
    if (max_streams_number == 0)
    {
        LOG_INFO(log, "There is a idle table named {}, no files need to parse.", getName());
        return updateFileInfos();
    }

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    auto new_context = Context::createCopy(getContext());

    InterpreterInsertQuery interpreter(insert, new_context, false, true, true);
    auto block_io = interpreter.execute();

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
            max_streams_number));
    }

    auto input= Pipe::unitePipes(std::move(pipes));

    assertBlocksHaveEqualStructure(input.getHeader(), block_io.pipeline.getHeader(), "StorageFileLog streamToViews");

    std::atomic<size_t> rows = 0;
    {
        block_io.pipeline.complete(std::move(input));
        block_io.pipeline.setNumThreads(max_streams_number);
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.", rows, table_id.getNameForLogs(), milliseconds);

    return updateFileInfos();
}

void StorageFileLog::wakeUp()
{
    std::unique_lock<std::mutex> lock(mutex);
    has_new_events = true;
    lock.unlock();
    cv.notify_one();
}

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

        auto physical_cpu_cores = getNumberOfPhysicalCPUCores();
        auto num_threads = filelog_settings->max_threads.value;

        if (!num_threads) /// Default
        {
            num_threads = std::max(1U, physical_cpu_cores / 4);
            filelog_settings->set("max_threads", num_threads);
        }
        else if (num_threads > physical_cpu_cores)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of threads to parse files can not be bigger than {}", physical_cpu_cores);
        }
        else if (num_threads < 1)
        {
            throw Exception("Number of threads to parse files can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (filelog_settings->max_block_size.changed && filelog_settings->max_block_size.value < 1)
        {
            throw Exception("filelog_max_block_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (filelog_settings->poll_max_batch_size.changed && filelog_settings->poll_max_batch_size.value < 1)
        {
            throw Exception("filelog_poll_max_batch_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        size_t init_sleep_time = filelog_settings->poll_directory_watch_events_backoff_init.totalMilliseconds();
        size_t max_sleep_time = filelog_settings->poll_directory_watch_events_backoff_max.totalMilliseconds();
        if (init_sleep_time > max_sleep_time)
        {
            throw Exception(
                "poll_directory_watch_events_backoff_init can not be greater than poll_directory_watch_events_backoff_max",
                ErrorCodes::BAD_ARGUMENTS);
        }

        if (filelog_settings->poll_directory_watch_events_backoff_factor.changed
            && !filelog_settings->poll_directory_watch_events_backoff_factor.value)
            throw Exception("poll_directory_watch_events_backoff_factor can not be 0", ErrorCodes::BAD_ARGUMENTS);

        if (args_count != 2)
            throw Exception(
                "Arguments size of StorageFileLog should be 2, path and format name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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
            args.attach);
    };

    factory.registerStorage(
        "FileLog",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}

bool StorageFileLog::updateFileInfos()
{
    if (file_infos.file_names.empty())
        return false;

    if (!directory_watch)
    {
        /// For table just watch one file, we can not use directory monitor to watch it
        if (!path_is_directory)
        {
            assert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
            assert(file_infos.file_names.size() == file_infos.context_by_name.size());
            assert(file_infos.file_names.size() == 1);

            if (auto it = file_infos.context_by_name.find(file_infos.file_names[0]); it != file_infos.context_by_name.end())
            {
                it->second.status = FileStatus::UPDATED;
                return true;
            }
        }
        return false;
    }
    /// Do not need to hold file_status lock, since it will be holded
    /// by caller when call this function
    auto error = directory_watch->getErrorAndReset();
    if (error.has_error)
        LOG_ERROR(log, "Error happened during watching directory {}: {}", directory_watch->getPath(), error.error_msg);

    /// These file infos should always have same size(one for one) before update and after update
    assert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
    assert(file_infos.file_names.size() == file_infos.context_by_name.size());

    auto events = directory_watch->getEventsAndReset();

    for (const auto & [file_name, event_infos] : events)
    {
        String file_path = getFullDataPath(file_name);
        for (const auto & event_info : event_infos.file_events)
        {
            switch (event_info.type)
            {
                case DirectoryWatcherBase::DW_ITEM_ADDED:
                {
                    LOG_TRACE(log, "New event {} watched, file_name: {}", event_info.callback, file_name);
                    /// Check if it is a regular file, and new file may be renamed or removed
                    if (std::filesystem::is_regular_file(file_path))
                    {
                        auto inode = getInode(file_path);

                        file_infos.file_names.push_back(file_name);

                        if (auto it = file_infos.meta_by_inode.find(inode); it != file_infos.meta_by_inode.end())
                            it->second = FileMeta{.file_name = file_name};
                        else
                            file_infos.meta_by_inode.emplace(inode, FileMeta{.file_name = file_name});

                        if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                            it->second = FileContext{.status = FileStatus::OPEN, .inode = inode};
                        else
                            file_infos.context_by_name.emplace(file_name, FileContext{.inode = inode});
                    }
                    break;
                }

                case DirectoryWatcherBase::DW_ITEM_MODIFIED:
                {
                    LOG_TRACE(log, "New event {} watched, file_name: {}", event_info.callback, file_name);
                    /// When new file added and appended, it has two event: DW_ITEM_ADDED
                    /// and DW_ITEM_MODIFIED, since the order of these two events in the
                    /// sequence is uncentain, so we may can not find it in file_infos, just
                    /// skip it, the file info will be handled in DW_ITEM_ADDED case.
                    if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                        it->second.status = FileStatus::UPDATED;
                    break;
                }

                case DirectoryWatcherBase::DW_ITEM_REMOVED:
                case DirectoryWatcherBase::DW_ITEM_MOVED_FROM:
                {
                    LOG_TRACE(log, "New event {} watched, file_name: {}", event_info.callback, file_name);
                    if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                        it->second.status = FileStatus::REMOVED;
                    break;
                }
                case DirectoryWatcherBase::DW_ITEM_MOVED_TO:
                {
                    LOG_TRACE(log, "New event {} watched, file_name: {}", event_info.callback, file_name);

                    /// Similar to DW_ITEM_ADDED, but if it removed from an old file
                    /// should obtain old meta file and rename meta file
                    if (std::filesystem::is_regular_file(file_path))
                    {
                        file_infos.file_names.push_back(file_name);
                        auto inode = getInode(file_path);

                        if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                            it->second = FileContext{.inode = inode};
                        else
                            file_infos.context_by_name.emplace(file_name, FileContext{.inode = inode});

                        /// File has been renamed, we should also rename meta file
                        if (auto it = file_infos.meta_by_inode.find(inode); it != file_infos.meta_by_inode.end())
                        {
                            auto old_name = it->second.file_name;
                            it->second.file_name = file_name;
                            if (std::filesystem::exists(getFullMetaPath(old_name)))
                                std::filesystem::rename(getFullMetaPath(old_name), getFullMetaPath(file_name));
                        }
                        /// May move from other place, adding new meta info
                        else
                            file_infos.meta_by_inode.emplace(inode, FileMeta{.file_name = file_name});
                    }
                }
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

                if (std::filesystem::exists(getFullMetaPath(file_name)))
                    std::filesystem::remove(getFullMetaPath(file_name));
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
    assert(file_infos.file_names.size() == file_infos.meta_by_inode.size());
    assert(file_infos.file_names.size() == file_infos.context_by_name.size());

    return events.empty() || file_infos.file_names.empty();
}

NamesAndTypesList StorageFileLog::getVirtuals() const
{
    return NamesAndTypesList{
        {"_filename", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_offset", std::make_shared<DataTypeUInt64>()}};
}

Names StorageFileLog::getVirtualColumnNames()
{
    return {"_filename", "_offset"};
}
}
