#include <filesystem>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Settings.h>
#include <Disks/IDisk.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Set.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Storages/SetSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSet.h>
#include <Common/StringUtils.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Interpreters/MutationsInterpreter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace fs = std::filesystem;


namespace DB
{

namespace SetSetting
{
    extern const SetSettingsString disk;
    extern const SetSettingsBool persistent;
}

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
    extern const int DEADLOCK_AVOIDED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

class SetOrJoinSink : public SinkToStorage, WithContext
{
public:
    SetOrJoinSink(
        ContextPtr ctx, StorageSetOrJoinBase & table_, const StorageMetadataPtr & metadata_snapshot_,
        const String & backup_path_, const String & backup_tmp_path_,
        const String & backup_file_name_, bool persistent_);
    ~SetOrJoinSink() override;

    String getName() const override { return "SetOrJoinSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void cancelBuffers() noexcept;

    StorageSetOrJoinBase & table;
    StorageMetadataPtr metadata_snapshot;
    String backup_path;
    String backup_tmp_path;
    String backup_file_name;
    std::unique_ptr<WriteBufferFromFileBase> backup_buf;
    CompressedWriteBuffer compressed_backup_buf;
    NativeWriter backup_stream;
    bool persistent;
};


SetOrJoinSink::SetOrJoinSink(
    ContextPtr ctx,
    StorageSetOrJoinBase & table_,
    const StorageMetadataPtr & metadata_snapshot_,
    const String & backup_path_,
    const String & backup_tmp_path_,
    const String & backup_file_name_,
    bool persistent_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , WithContext(ctx)
    , table(table_)
    , metadata_snapshot(metadata_snapshot_)
    , backup_path(backup_path_)
    , backup_tmp_path(backup_tmp_path_)
    , backup_file_name(backup_file_name_)
    , backup_buf(table_.disk->writeFile(fs::path(backup_tmp_path) / backup_file_name))
    , compressed_backup_buf(*backup_buf)
    , backup_stream(compressed_backup_buf, 0, metadata_snapshot->getSampleBlock())
    , persistent(persistent_)
{
}

SetOrJoinSink::~SetOrJoinSink()
{
    if (isCancelled())
        cancelBuffers();
}

void SetOrJoinSink::cancelBuffers() noexcept
{
    compressed_backup_buf.cancel();
    if (backup_buf)
        backup_buf->cancel();
}


void SetOrJoinSink::consume(Chunk & chunk)
{
    Block block = getHeader().cloneWithColumns(chunk.getColumns());

    table.insertBlock(block, getContext());
    if (persistent)
        backup_stream.write(block);
}

void SetOrJoinSink::onFinish()
{
    table.finishInsert(getContext());
    if (persistent)
    {
        backup_stream.flush();
        compressed_backup_buf.finalize();
        backup_buf->finalize();

        table.disk->replaceFile(fs::path(backup_tmp_path) / backup_file_name, fs::path(backup_path) / backup_file_name);
    }
    else
    {
        cancelBuffers();
    }
}


SinkToStoragePtr StorageSetOrJoinBase::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    UInt64 id = ++increment;
    return std::make_shared<SetOrJoinSink>(
        context, *this, metadata_snapshot, path, fs::path(path) / "tmp/", toString(id) + ".bin", persistent);
}


StorageSetOrJoinBase::StorageSetOrJoinBase(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : IStorage(table_id_), disk(disk_), persistent(persistent_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Join and Set storages require data path");

    path = relative_path_;
}


StorageSet::StorageSet(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : StorageSetOrJoinBase{disk_, relative_path_, table_id_, columns_, constraints_, comment, persistent_}
    , set(std::make_shared<Set>(SizeLimits(), 0, true))
{
    Block header = getInMemoryMetadataPtr()->getSampleBlock();
    set->setHeader(header.getColumnsWithTypeAndName());
    set->fillSetElements();

    restore();
    set->finishInsert();
}

RWLockImpl::LockHolder StorageSet::tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef()[Setting::lock_acquire_timeout] : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return tryLockTimed(lock, type, query_id, acquire_timeout);
}

RWLockImpl::LockHolder StorageSet::tryLockForCurrentQueryTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context)
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef()[Setting::lock_acquire_timeout] : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return lock->getLock(type, query_id, acquire_timeout, false);
}

SinkToStoragePtr
StorageSet::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    std::lock_guard mutate_lock(mutex);
    return StorageSetOrJoinBase::write(query, metadata_snapshot, context, /*async_insert=*/false);
}

SetPtr StorageSet::getSet() const
{
    std::lock_guard lock(mutex);
    return set;
}


void StorageSet::insertBlock(const Block & block, ContextPtr context)
{
    TableLockHolder holder = tryLockForCurrentQueryTimedWithContext(rwlock, RWLockImpl::Write, context);

    if (!holder)
        throw Exception(
            ErrorCodes::DEADLOCK_AVOIDED, "StorageSet: cannot insert data because current query tries to read from this storage");

    set->insertFromBlock(block.getColumnsWithTypeAndName());
}

void StorageSet::finishInsert(ContextPtr context)
{
    TableLockHolder holder = tryLockForCurrentQueryTimedWithContext(rwlock, RWLockImpl::Write, context);

    if (!holder)
        throw Exception(
            ErrorCodes::DEADLOCK_AVOIDED, "StorageSet: cannot insert data because current query tries to read from this storage");
    set->finishInsert();
}

size_t StorageSet::getSize(ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return set->getTotalRowCount();
}

std::optional<UInt64> StorageSet::totalRows(ContextPtr query_context) const
{
    const auto & settings = query_context->getSettingsRef();
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings[Setting::lock_acquire_timeout]);
    return set->getTotalRowCount();
}

std::optional<UInt64> StorageSet::totalBytes(ContextPtr query_context) const
{
    const auto & settings = query_context->getSettingsRef();
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings[Setting::lock_acquire_timeout]);
    return set->getTotalByteCount();
}

void StorageSet::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, TableExclusiveLockHolder &)
{
    std::lock_guard mutate_lock(mutex);
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    if (disk->existsDirectory(path))
        disk->removeRecursive(path);
    else
        LOG_INFO(getLogger("StorageSet"), "Path {} is already removed from disk {}", path, disk->getName());

    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    Block header = metadata_snapshot->getSampleBlock();

    increment = 0;

    set = std::make_shared<Set>(SizeLimits(), 0, true);
    set->setHeader(header.getColumnsWithTypeAndName());
    set->fillSetElements();
    set->finishInsert();
}

void StorageSet::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    for (const auto & command : commands)
        if (command.type != MutationCommand::DELETE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine Set supports only DELETE mutations");
}

void StorageSet::mutate(const MutationCommands & commands, ContextPtr context)
{
    std::lock_guard mutate_lock(mutex);

    constexpr auto tmp_backup_file_name = "tmp/mut.bin";
    auto metadata_snapshot = getInMemoryMetadataPtr();

    auto backup_buf = disk->writeFile(path + tmp_backup_file_name);
    auto compressed_backup_buf = CompressedWriteBuffer(*backup_buf);
    auto backup_stream = NativeWriter(compressed_backup_buf, 0, metadata_snapshot->getSampleBlock());

    auto new_set = std::make_shared<Set>(SizeLimits(), 0, true);
    new_set->setHeader(metadata_snapshot->getSampleBlock().getColumnsWithTypeAndName());
    new_set->fillSetElements();

    // New scope controls lifetime of pipeline.
    {
        auto storage_ptr = DatabaseCatalog::instance().getTable(getStorageID(), context);
        MutationsInterpreter::Settings settings(true);
        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
        {
            new_set->insertFromBlock(block.getColumnsWithTypeAndName());
            if (persistent)
                backup_stream.write(block);
        }
    }

    new_set->finishInsert();
    /// Now acquire exclusive lock and modify storage.
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    set = std::move(new_set);
    increment = 1;

    if (persistent)
    {
        backup_stream.flush();
        compressed_backup_buf.finalize();
        backup_buf->finalize();

        std::vector<std::string> files;
        disk->listFiles(path, files);
        for (const auto & file_name: files)
        {
            if (file_name.ends_with(".bin"))
                disk->removeFileIfExists(path + file_name);
        }

        disk->replaceFile(path + tmp_backup_file_name, path + std::to_string(increment) + ".bin");
    }
    else
    {
        compressed_backup_buf.cancel();
        backup_buf->cancel();
    }
}

void StorageSetOrJoinBase::restore()
{
    if (!disk->existsDirectory(fs::path(path) / "tmp"))
    {
        disk->createDirectories(fs::path(path) / "tmp");
        return;
    }

    static const char * file_suffix = ".bin";
    static const auto file_suffix_size = strlen(".bin");

    using FilePriority = std::pair<UInt64, String>;
    std::priority_queue<FilePriority, std::vector<FilePriority>, std::greater<>> backup_files;
    for (auto dir_it{disk->iterateDirectory(path)}; dir_it->isValid(); dir_it->next())
    {
        const auto & name = dir_it->name();
        const auto & file_path = dir_it->path();

        if (disk->existsFile(file_path)
            && endsWith(name, file_suffix)
            && disk->getFileSize(file_path) > 0)
        {
            /// Calculate the maximum number of available files with a backup to add the following files with large numbers.
            UInt64 file_num = parse<UInt64>(name.substr(0, name.size() - file_suffix_size));
            if (file_num > increment)
                increment = file_num;

            backup_files.push({file_num, file_path});
        }
    }

    /// Restore in the same order as blocks were written
    /// It may be important for storage Join, user expect to get the first row (unless `join_any_take_last_row` setting is set)
    /// but after restart we may have different order of blocks in memory.
    while (!backup_files.empty())
    {
        restoreFromFile(backup_files.top().second);
        backup_files.pop();
    }
}


void StorageSetOrJoinBase::restoreFromFile(const String & file_path)
{
    ContextPtr ctx = nullptr;
    auto backup_buf = disk->readFile(file_path, getReadSettings());
    CompressedReadBuffer compressed_backup_buf(*backup_buf);
    NativeReader backup_stream(compressed_backup_buf, 0);

    ProfileInfo info;
    while (Block block = backup_stream.read())
    {
        info.update(block);
        insertBlock(block, ctx);
    }

    finishInsert(ctx);

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(getLogger("StorageSetOrJoinBase"), "Loaded from backup file {}. {} rows, {}. State has {} unique rows.",
        file_path, info.rows, ReadableSize(info.bytes), getSize(ctx));
}


void StorageSetOrJoinBase::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    /// Rename directory with data.
    disk->replaceFile(path, new_path_to_table_data);

    path = new_path_to_table_data;
    renameInMemory(new_table_id);
}

class SetSource : public ISource
{
public:
    SetSource(SetPtr set_, TableLockHolder lock_holder_, UInt64 max_block_size_, ColumnNumbers column_numbers_, Block sample_block_)
        : ISource(sample_block_)
        , set(set_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , column_numbers(std::move(column_numbers_))
        , sample_block(std::move(sample_block_))
    {
    }

    String getName() const override { return "Set"; }

protected:
    Chunk generate() override
    {
        if (!set->hasExplicitSetElements() || set->empty() || current_position >= set->getTotalRowCount())
            return {};

        const size_t total_rows = set->getTotalRowCount();
        const size_t rows_to_read = std::min(max_block_size, total_rows - current_position);

        MutableColumns mut_columns = sample_block.cloneEmpty().mutateColumns();

        for (size_t idx = 0; idx < column_numbers.size(); ++idx)
        {
            const auto & set_column = *set->getSetElements()[column_numbers[idx]];
            auto & res_column = mut_columns[idx];

            for (size_t row = 0; row < rows_to_read; ++row)
            {
                res_column->insert(set_column[current_position + row]);
            }
        }

        current_position += rows_to_read;

        return Chunk(std::move(mut_columns), rows_to_read);
    }

private:
    SetPtr set;
    TableLockHolder lock_holder;

    UInt64 current_position = 0;
    UInt64 max_block_size;
    ColumnNumbers column_numbers;
    Block sample_block;
};

Pipe StorageSet::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block source_sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    ColumnNumbers column_numbers = storage_snapshot->getColumnNumbersByNames(column_names);

    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return Pipe(std::make_shared<SetSource>(set, std::move(holder), max_block_size, column_numbers, source_sample_block));
}

void registerStorageSet(StorageFactory & factory)
{
    factory.registerStorage("Set", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        bool has_settings = args.storage_def->settings;
        SetSettings set_settings;
        if (has_settings)
            set_settings.loadFromQuery(*args.storage_def);

        DiskPtr disk = args.getContext()->getDisk(set_settings[SetSetting::disk]);
        return std::make_shared<StorageSet>(
            disk, args.relative_data_path, args.table_id, args.columns, args.constraints, args.comment, set_settings[SetSetting::persistent]);
    }, StorageFactory::StorageFeatures{ .supports_settings = true, .has_builtin_setting_fn = SetSettings::hasBuiltin, });
}

}
