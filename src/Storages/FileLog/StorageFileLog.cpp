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
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Pipe.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/FileLog/FileLogSource.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Poco/File.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>

#include <sys/stat.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_GET_FILE_STAT;
    extern const int NOT_REGULAR_FILE;
    extern const int READ_META_FILE_FAILED;
    extern const int FILE_STREAM_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{
    const auto RESCHEDULE_MS = 500;
    const auto BACKOFF_TRESHOLD = 32000;
    const auto MAX_THREAD_WORK_DURATION_MS = 60000;
}

StorageFileLog::StorageFileLog(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & relative_path_,
    const String & format_name_,
    std::unique_ptr<FileLogSettings> settings,
    bool attach)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , filelog_settings(std::move(settings))
    , path(getContext()->getUserFilesPath() + "/" + relative_path_)
    , format_name(format_name_)
    , log(&Poco::Logger::get("StorageFileLog (" + table_id_.table_name + ")"))
    , milliseconds_to_wait(RESCHEDULE_MS)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    try
    {
        loadMetaFiles(attach);
        loadFiles();

        assert(
            file_infos.file_names.size() == file_infos.meta_by_inode.size() == file_infos.inode_by_name.size()
            == file_infos.context_by_name.size());

        if (path_is_directory)
            directory_watch = std::make_unique<FileLogDirectoryWatcher>(path);

        auto thread = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this] { threadFunc(); });
        task = std::make_shared<TaskContext>(std::move(thread));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageFileLog::loadMetaFiles(bool attach)
{
    const auto database = DatabaseCatalog::instance().getDatabase(getStorageID().getDatabaseName());
    const auto table_name = getStorageID().getTableName();

    root_meta_path = database->getMetadataPath() + "/." + table_name;

    /// Create table, just create meta data directory
    if (!attach)
    {
        if (std::filesystem::exists(root_meta_path))
        {
            std::filesystem::remove_all(root_meta_path);
        }
        std::filesystem::create_directories(root_meta_path);
    }
    /// Attach table
    else
    {
        /// Meta file may lost, log and create directory
        if (!std::filesystem::exists(root_meta_path))
        {
            LOG_INFO(log, "Meta files of table {} may have lost.", getStorageID().getTableName());
            std::filesystem::create_directories(root_meta_path);
        }
        /// Load all meta info to file_infos;
        deserialize();
    }
}

void StorageFileLog::loadFiles()
{

    if (std::filesystem::is_regular_file(path))
    {
        path_is_directory = false;
        root_data_path = getContext()->getUserFilesPath();

        file_infos.file_names.push_back(std::filesystem::path(path).filename());
    }
    else if (std::filesystem::is_directory(path))
    {
        root_data_path = path;
        /// Just consider file with depth 1
        for (const auto & dir_entry : std::filesystem::directory_iterator{path})
        {
            if (dir_entry.is_regular_file())
            {
                file_infos.file_names.push_back(dir_entry.path().filename());
            }
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The path {} neither a regular file, nor a directory", path);
    }

    /// Get files inode
    for (const auto & file : file_infos.file_names)
    {
        auto inode = getInode(getFullDataPath(file));
        file_infos.inode_by_name.emplace(file, inode);
        file_infos.context_by_name.emplace(file, FileContext{});
    }

    /// Update file meta or create file meta
    for (const auto & file_inode : file_infos.inode_by_name)
    {
        if (auto it = file_infos.meta_by_inode.find(file_inode.second); it != file_infos.meta_by_inode.end())
        {
            /// data file have been renamed, need update meta file's name
            if (it->second.file_name != file_inode.first)
            {
                it->second.file_name = file_inode.first;
                if (std::filesystem::exists(getFullMetaPath(it->second.file_name)))
                {
                    std::filesystem::rename(getFullMetaPath(it->second.file_name), getFullMetaPath(file_inode.first));
                }
            }
        }
        /// New file
        else
        {
            FileMeta meta{file_inode.first, 0, 0};
            file_infos.meta_by_inode.emplace(file_inode.second, meta);
        }
    }

    /// Clear unneeded meta file, because data files may be deleted
    if (file_infos.meta_by_inode.size() > file_infos.inode_by_name.size())
    {
        InodeToFileMeta valid_metas;
        valid_metas.reserve(file_infos.inode_by_name.size());
        for (const auto & it : file_infos.meta_by_inode)
        {
            if (file_infos.inode_by_name.contains(it.second.file_name))
                valid_metas.emplace(it);
        }
        file_infos.meta_by_inode.swap(valid_metas);
    }
}

void StorageFileLog::serialize(bool with_end_pos) const
{
    for (const auto & it : file_infos.meta_by_inode)
    {
        auto full_name = getFullMetaPath(it.second.file_name);
        if (!std::filesystem::exists(full_name))
        {
            Poco::File{full_name}.createFile();
        }
        WriteBufferFromFile buf(full_name);
        writeIntText(it.first, buf);
        writeChar('\n', buf);
        writeIntText(it.second.last_writen_position, buf);

        if (with_end_pos)
        {
            writeChar('\n', buf);
            writeIntText(it.second.last_open_end, buf);
        }
    }
}

void StorageFileLog::deserialize()
{
    for (const auto & dir_entry : std::filesystem::directory_iterator{root_meta_path})
    {
        if (!dir_entry.is_regular_file())
        {
            throw Exception(
                ErrorCodes::NOT_REGULAR_FILE,
                "The file {} under {} is not a regular file when deserializing meta files.",
                dir_entry.path().c_str(),
                root_meta_path);
        }

        ReadBufferFromFile buf(dir_entry.path().c_str());
        FileMeta meta;
        UInt64 inode, last_written_pos;

        if (!tryReadIntText(inode, buf))
        {
            throw Exception(ErrorCodes::READ_META_FILE_FAILED, "Read meta file {} failed.", dir_entry.path().c_str());
        }
        if (!checkChar('\n', buf))
        {
            throw Exception(ErrorCodes::READ_META_FILE_FAILED, "Read meta file {} failed.", dir_entry.path().c_str());
        }
        if (!tryReadIntText(last_written_pos, buf))
        {
            throw Exception(ErrorCodes::READ_META_FILE_FAILED, "Read meta file {} failed.", dir_entry.path().c_str());
        }

        meta.file_name = dir_entry.path().filename();
        meta.last_writen_position = last_written_pos;

        /// May have last open end in meta file
        if (checkChar('\n', buf))
        {
            if (!tryReadIntText(meta.last_open_end, buf))
            {
                throw Exception(ErrorCodes::READ_META_FILE_FAILED, "Read meta file {} failed.", dir_entry.path().c_str());
            }
        }

        file_infos.meta_by_inode.emplace(inode, meta);
    }
}

UInt64 StorageFileLog::getInode(const String & file_name)
{
    struct stat file_stat;
    if (stat(file_name.c_str(), &file_stat))
    {
        throw Exception(ErrorCodes::CANNOT_GET_FILE_STAT, "Can not get stat info of file {}", file_name);
    }
    return file_stat.st_ino;
}

Pipe StorageFileLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    std::lock_guard<std::mutex> lock(status_mutex);

    updateFileInfos();

    /// No files to parse
    if (file_infos.file_names.empty())
    {
        return Pipe{};
    }

    auto modified_context = Context::createCopy(local_context);

    auto max_streams_number = std::min<UInt64>(filelog_settings->filelog_max_threads, file_infos.file_names.size());

    Pipes pipes;
    pipes.reserve(max_streams_number);
    for (size_t stream_number = 0; stream_number < max_streams_number; ++stream_number)
    {
        Pipe pipe(std::make_shared<FileLogSource>(
            *this, metadata_snapshot, modified_context, getMaxBlockSize(), getPollTimeoutMillisecond(), stream_number, max_streams_number));

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID()).getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        auto actions = std::make_shared<ExpressionActions>(
            convert_actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

        pipe.addSimpleTransform([&](const Block & stream_header) { return std::make_shared<ExpressionTransform>(stream_header, actions); });
        pipes.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(pipes));
}

void StorageFileLog::drop()
{
    if (std::filesystem::exists(root_meta_path))
        std::filesystem::remove_all(root_meta_path);
}

void StorageFileLog::startup()
{
    if (task)
    {
        task->holder->activateAndSchedule();
    }
}

void StorageFileLog::shutdown()
{
    if (task)
    {
        task->stream_cancelled = true;

        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
    }
    closeFilesAndStoreMeta();
}

void StorageFileLog::openFilesAndSetPos()
{
    for (const auto & file : file_infos.file_names)
    {
        auto & file_ctx = file_infos.context_by_name.at(file);
        if (file_ctx.status != FileStatus::NO_CHANGE)
        {
            file_ctx.reader.open(getFullDataPath(file));
            if (!file_ctx.reader.good())
            {
                throw Exception(ErrorCodes::FILE_STREAM_ERROR, "Open file {} failed.", file);
            }

            file_ctx.reader.seekg(0, file_ctx.reader.end);
            if (!file_ctx.reader.good())
            {
                throw Exception(ErrorCodes::FILE_STREAM_ERROR, "Seekg file {} failed.", file);
            }

            auto file_end = file_ctx.reader.tellg();
            if (!file_ctx.reader.good())
            {
                throw Exception(ErrorCodes::FILE_STREAM_ERROR, "Tellg file {} failed.", file);
            }

            auto & meta = file_infos.meta_by_inode.at(file_infos.inode_by_name.at(file));
            if (meta.last_writen_position > static_cast<UInt64>(file_end))
            {
                throw Exception(ErrorCodes::FILE_STREAM_ERROR, "File {} has been broken.", file);
            }
            /// update file end at the monment, used in ReadBuffer and serialize
            meta.last_open_end = file_end;

            file_ctx.reader.seekg(meta.last_writen_position);
            if (!file_ctx.reader.good())
            {
                throw Exception(ErrorCodes::FILE_STREAM_ERROR, "Seekg file {} failed.", file);
            }
        }
    }
    serialize(true);
}

void StorageFileLog::closeFilesAndStoreMeta()
{
    for (auto & it : file_infos.context_by_name)
    {
        if (it.second.reader.is_open())
            it.second.reader.close();
    }
    serialize();
}

size_t StorageFileLog::getMaxBlockSize() const
{
    return filelog_settings->filelog_max_block_size.changed ? filelog_settings->filelog_max_block_size.value
                                                            : getContext()->getSettingsRef().max_insert_block_size.value;
}

size_t StorageFileLog::getPollMaxBatchSize() const
{
    size_t batch_size = filelog_settings->filelog_poll_max_batch_size.changed ? filelog_settings->filelog_poll_max_batch_size.value
                                                                              : getContext()->getSettingsRef().max_block_size.value;
    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageFileLog::getPollTimeoutMillisecond() const
{
    return filelog_settings->filelog_poll_timeout_ms.changed ? filelog_settings->filelog_poll_timeout_ms.totalMilliseconds()
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

void StorageFileLog::threadFunc()
{
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                if (streamToViews())
                {
                    LOG_TRACE(log, "Stream stalled. Reschedule.");
                    if (milliseconds_to_wait < BACKOFF_TRESHOLD)
                        milliseconds_to_wait *= 2;
                    break;
                }
                else
                {
                    milliseconds_to_wait = RESCHEDULE_MS;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    break;
                }
                updateFileInfos();
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    // Wait for attached views
    if (!task->stream_cancelled)
        task->holder->scheduleAfter(milliseconds_to_wait);
}

bool StorageFileLog::streamToViews()
{
    std::lock_guard<std::mutex> lock(status_mutex);
    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    auto max_streams_number = std::min<UInt64>(filelog_settings->filelog_max_threads.value, file_infos.file_names.size());
    /// No files to parse
    if (max_streams_number == 0)
    {
        return false;
    }

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    auto new_context = Context::createCopy(getContext());

    InterpreterInsertQuery interpreter(insert, new_context, false, true, true);
    auto block_io = interpreter.execute();

    Pipes pipes;
    pipes.reserve(max_streams_number);
    for (size_t stream_number = 0; stream_number < max_streams_number; ++stream_number)
    {
        Pipe pipe(std::make_shared<FileLogSource>(
            *this, metadata_snapshot, new_context, getPollMaxBatchSize(), getPollTimeoutMillisecond(), stream_number, max_streams_number));

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            block_io.out->getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        auto actions = std::make_shared<ExpressionActions>(
            convert_actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

        pipe.addSimpleTransform([&](const Block & stream_header) { return std::make_shared<ExpressionTransform>(stream_header, actions); });
        pipes.emplace_back(std::move(pipe));
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));

    assertBlocksHaveEqualStructure(pipeline.getHeader(), block_io.out->getHeader(), "StorageFileLog streamToViews");

    size_t rows = 0;

    PullingPipelineExecutor executor(pipeline);
    Block block;
    block_io.out->writePrefix();
    while (executor.pull(block))
    {
        block_io.out->write(block);
        rows += block.rows();
        /// During files open, also save file end at the opening moment
        serialize(true);
    }
    block_io.out->writeSuffix();

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.",
        formatReadableQuantity(rows), table_id.getNameForLogs(), milliseconds);

    return updateFileInfos();
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
        auto num_threads = filelog_settings->filelog_max_threads.value;

        if (num_threads > physical_cpu_cores)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of threads to parse files can not be bigger than {}", physical_cpu_cores);
        }
        else if (num_threads < 1)
        {
            throw Exception("Number of threads to parse files can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (filelog_settings->filelog_max_block_size.changed && filelog_settings->filelog_max_block_size.value < 1)
        {
            throw Exception("filelog_max_block_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (filelog_settings->filelog_poll_max_batch_size.changed && filelog_settings->filelog_poll_max_batch_size.value < 1)
        {
            throw Exception("filelog_poll_max_batch_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (args_count != 2)
            throw Exception(
                "Arguments size of StorageFileLog should be 2, path and format name", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto path_ast = evaluateConstantExpressionAsLiteral(engine_args[0], args.getContext());
        auto format_ast = evaluateConstantExpressionAsLiteral(engine_args[1], args.getContext());

        auto path = path_ast->as<ASTLiteral &>().value.safeGet<String>();
        auto format = format_ast->as<ASTLiteral &>().value.safeGet<String>();

        return StorageFileLog::create(
            args.table_id, args.getContext(), args.columns, path, format, std::move(filelog_settings), args.attach);
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
    if (!directory_watch)
    {
        /// For table just watch one file, we can not use directory monitor to watch it
        if (!path_is_directory)
        {
            assert(
                file_infos.file_names.size() == file_infos.meta_by_inode.size() == file_infos.inode_by_name.size()
                == file_infos.context_by_name.size() == 1);
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

    auto events = directory_watch->getEventsAndReset();

    for (const auto & event : events)
    {
        switch (event.type)
        {
            case Poco::DirectoryWatcher::DW_ITEM_ADDED: {
                LOG_TRACE(log, "New event {} watched, path: {}", event.callback, event.path);
                if (std::filesystem::is_regular_file(event.path))
                {
                    auto file_name = std::filesystem::path(event.path).filename();
                    auto inode = getInode(event.path);

                    file_infos.file_names.push_back(file_name);
                    file_infos.inode_by_name.emplace(file_name, inode);

                    FileMeta meta{file_name, 0, 0};
                    file_infos.meta_by_inode.emplace(inode, meta);
                    file_infos.context_by_name.emplace(file_name, FileContext{});
                }
                break;
            }

            case Poco::DirectoryWatcher::DW_ITEM_MODIFIED: {
                auto file_name = std::filesystem::path(event.path).filename();
                LOG_TRACE(log, "New event {} watched, path: {}", event.callback, event.path);
                if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                {
                    it->second.status = FileStatus::UPDATED;
                }
                break;
            }

            case Poco::DirectoryWatcher::DW_ITEM_REMOVED:
            case Poco::DirectoryWatcher::DW_ITEM_MOVED_FROM: {
                auto file_name = std::filesystem::path(event.path).filename();
                LOG_TRACE(log, "New event {} watched, path: {}", event.callback, event.path);
                if (auto it = file_infos.context_by_name.find(file_name); it != file_infos.context_by_name.end())
                {
                    it->second.status = FileStatus::REMOVED;
                }
                break;
            }
            /// file rename
            case Poco::DirectoryWatcher::DW_ITEM_MOVED_TO: {
                auto file_name = std::filesystem::path(event.path).filename();
                LOG_TRACE(log, "New event {} watched, path: {}", event.callback, event.path);

                file_infos.file_names.push_back(file_name);
                file_infos.context_by_name.emplace(file_name, FileContext{});

                auto inode = getInode(event.path);
                file_infos.inode_by_name.emplace(file_name, inode);

                if (auto it = file_infos.meta_by_inode.find(inode); it != file_infos.meta_by_inode.end())
                {
                    // rename meta file
                    auto old_name = it->second.file_name;
                    it->second.file_name = file_name;
                    if (std::filesystem::exists(getFullMetaPath(old_name)))
                    {
                        std::filesystem::rename(getFullMetaPath(old_name), getFullMetaPath(file_name));
                    }
                }
            }
        }
    }
    std::vector<String> valid_files;

    for (const auto & file_name : file_infos.file_names)
    {
        if (auto it = file_infos.context_by_name.find(file_name);
            it != file_infos.context_by_name.end() && it->second.status == FileStatus::REMOVED)
        {
            file_infos.context_by_name.erase(it);
            if (auto inode = file_infos.inode_by_name.find(file_name); inode != file_infos.inode_by_name.end())
            {
                file_infos.inode_by_name.erase(inode);
                file_infos.meta_by_inode.erase(inode->second);
                if (std::filesystem::exists(getFullMetaPath(file_name)))
                    std::filesystem::remove(getFullMetaPath(file_name));
            }
        }
        else
        {
            valid_files.push_back(file_name);
        }
    }
    file_infos.file_names.swap(valid_files);

    /// These file infos should always have same size(one for one)
    assert(
        file_infos.file_names.size() == file_infos.meta_by_inode.size() == file_infos.inode_by_name.size()
        == file_infos.context_by_name.size());

    return events.empty() || file_infos.file_names.empty();
}

NamesAndTypesList StorageFileLog::getVirtuals() const
{
    return NamesAndTypesList{{"_file_name", std::make_shared<DataTypeString>()}, {"_offset", std::make_shared<DataTypeUInt64>()}};
}

Names StorageFileLog::getVirtualColumnNames()
{
    return {"_file_name", "_offset"};
}
}
