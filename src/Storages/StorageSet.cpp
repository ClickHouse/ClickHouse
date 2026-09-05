#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/SetSettings.h>
#include <Storages/StorageSet.h>
#include <Storages/StorageFactory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Disks/IDisk.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/formatReadable.h>
#include <Common/StringUtils.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>
#include <Interpreters/Set.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Parsers/ASTCreateQuery.h>
#include <filesystem>
#include <optional>
#include <vector>

namespace fs = std::filesystem;


namespace DB
{

namespace SetSetting
{
    extern const SetSettingsString disk;
    extern const SetSettingsBool persistent;
}

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace FailPoints
{
    extern const char set_or_join_sink_pause_before_publish[];
    extern const char set_or_join_sink_pause_before_replay[];
}

class SetOrJoinSink final : public SinkToStorage, WithContext
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
    void onException(std::exception_ptr exception) override;

private:
    void cancelBuffers() noexcept;

    /// Cancel the buffers and remove the staged or promoted file. A failed `INSERT` must not
    /// leave data that would be restored after a restart.
    void discardStagedBackup() noexcept;

    StorageSetOrJoinBase & table;
    StorageMetadataPtr metadata_snapshot;
    String backup_path;
    String backup_tmp_path;
    String backup_file_name;
    std::unique_ptr<WriteBufferFromFileBase> backup_buf;
    std::optional<CompressedWriteBuffer> compressed_backup_buf;
    std::optional<NativeWriter> backup_stream;
    bool backup_promoted = false;
    /// Set when `onFinish` published the backup and the live state. Until then the staged file
    /// belongs to an unfinished `INSERT` and must be removed if the sink goes away.
    bool insert_finished = false;
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
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , WithContext(ctx)
    , table(table_)
    , metadata_snapshot(metadata_snapshot_)
    , backup_path(backup_path_)
    , backup_tmp_path(backup_tmp_path_)
    , backup_file_name(backup_file_name_)
    , persistent(persistent_)
{
}

SetOrJoinSink::~SetOrJoinSink()
{
    /// Do not look at `isCancelled` here: a pipeline is also cancelled after it has completed
    /// successfully, and the backup of a finished `INSERT` must survive that.
    if (!insert_finished)
    {
        /// The rollback of a concurrently failed insert reads the committed backup files, so
        /// removing one must not interleave with it.
        std::lock_guard publish_lock(table.mutate_mutex);
        discardStagedBackup();
    }
}

void SetOrJoinSink::cancelBuffers() noexcept
{
    if (compressed_backup_buf)
        compressed_backup_buf->cancel();
    if (backup_buf)
        backup_buf->cancel();
}

void SetOrJoinSink::discardStagedBackup() noexcept
{
    if (!backup_buf)
        return;

    cancelBuffers();

    /// The buffers are cancelled above, so nothing writes to the file after this point. Cancelling
    /// leaves nothing behind on object storage (the metadata file is created on finalize), but a
    /// local disk keeps whatever was flushed, so remove the file explicitly in both cases.
    try
    {
        const auto backup_file_path = fs::path(backup_promoted ? backup_path : backup_tmp_path) / backup_file_name;
        table.disk->removeFileIfExists(backup_file_path);
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("SetOrJoinSink"),
            fmt::format(
                "Cannot remove the staged backup file {} of table {} on disk {}",
                fs::path(backup_promoted ? backup_path : backup_tmp_path) / backup_file_name,
                table.getStorageID().getNameForLogs(),
                table.disk->getName()));
    }

    backup_stream.reset();
    compressed_backup_buf.reset();
    backup_buf.reset();
}

void SetOrJoinSink::onException(std::exception_ptr)
{
    /// The same critical section as the publish phase in `onFinish`: the rollback of a concurrently
    /// failed insert rebuilds the live state from the committed backup files, so removing the
    /// staged or promoted file must not interleave with it.
    std::lock_guard publish_lock(table.mutate_mutex);

    /// If the failure happened while `publishBackup` was replaying the promoted backup, the live
    /// state has already been restored and the promoted file removed (`publishBackup` gives the
    /// strong exception guarantee), so only the file of an earlier failure remains to clean up.
    discardStagedBackup();
}


void SetOrJoinSink::consume(Chunk & chunk)
{
    Block block = getHeader().cloneWithColumns(chunk.getColumns());

    /// Stage blocks in a temporary backup file and keep them out of the live state until the whole
    /// file has been finalized and promoted. The backup is replayed in `onFinish`, so an INSERT
    /// does not retain a second in-memory copy of all its input blocks.
    if (persistent)
    {
        if (!backup_buf)
        {
            /// The staged blocks are published to the live state only in `onFinish`. Check right away
            /// that this query is allowed to update the state, so a query that reads from the same
            /// table fails as early as it did when blocks were inserted one by one.
            table.checkInsertIsPossible(getContext());

            backup_buf = table.disk->writeFile(fs::path(backup_tmp_path) / backup_file_name);
            compressed_backup_buf.emplace(*backup_buf);
            backup_stream.emplace(*compressed_backup_buf, 0, std::make_shared<const Block>(metadata_snapshot->getSampleBlock()));
        }
        backup_stream->write(block);
        return;
    }

    table.insertBlock(block, getContext());
}

void SetOrJoinSink::onFinish()
{
    if (backup_buf)
    {
        FailPointInjection::pauseFailPoint(FailPoints::set_or_join_sink_pause_before_publish);

        /// The whole publish phase (promote the staged file, then replay it into the live state)
        /// must be atomic with respect to other persistent inserts and their rollbacks: if the
        /// rollback of a concurrently failed insert rebuilt the live state from the committed
        /// backups while this insert is still replaying its own committed backup, the remaining
        /// blocks of the replay would be applied on top of a state that already contains them.
        std::lock_guard publish_lock(table.mutate_mutex);

        /// Fail before the staged file is promoted: publishing it and only then discovering that the
        /// live state cannot be updated would leave the backup and the state out of sync.
        table.checkInsertIsPossible(getContext());

        backup_stream->flush();
        compressed_backup_buf->finalize();
        backup_buf->finalize();

        table.disk->replaceFile(fs::path(backup_tmp_path) / backup_file_name, fs::path(backup_path) / backup_file_name);
        backup_promoted = true;

        FailPointInjection::pauseFailPoint(FailPoints::set_or_join_sink_pause_before_replay);

        /// Replays the promoted backup into the live state with the strong exception guarantee:
        /// if the replay fails, the state is restored and the promoted file removed before the
        /// exception leaves the publish critical section.
        table.publishBackup(fs::path(backup_path) / backup_file_name, getContext());
    }
    else
    {
        table.finishInsert();
    }

    insert_finished = true;
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
    : StorageWithCommonVirtualColumns(table_id_), disk(disk_), persistent(persistent_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Join and Set storages require data path");

    path = relative_path_;
}

VirtualColumnsDescription StorageSetOrJoinBase::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
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
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    Block header = metadata_snapshot->getSampleBlock();
    set->setHeader(header.getColumnsWithTypeAndName());

    restore();
}


SetPtr StorageSet::getSet() const
{
    std::lock_guard lock(mutex);
    return set;
}


void StorageSet::insertBlock(const Block & block, ContextPtr)
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    current_set->insertFromBlock(block.getColumnsWithTypeAndName());
}

void StorageSet::finishInsert()
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    current_set->finishInsert();
}

void StorageSet::publishBackup(const String & backup_file_path, ContextPtr context)
{
    try
    {
        restoreFromFile(backup_file_path, context);
    }
    catch (...)
    {
        /// Restore the previous live state while still inside the publish critical section, so no
        /// concurrent publish or rollback can replay the backup of this failed insert. The file is
        /// removed first because the rebuild reads all committed backups.
        try
        {
            disk->removeFileIfExists(backup_file_path);
            rebuildFromBackups();
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("StorageSet"),
                fmt::format("Cannot restore the in-memory state of table {} after a failed INSERT", getStorageID().getNameForLogs()));
        }
        throw;
    }
}

void StorageSet::rebuildFromBackups()
{
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    auto rebuilt_set = std::make_shared<Set>(SizeLimits(), 0, true);
    rebuilt_set->setHeader(metadata_snapshot->getSampleBlock().getColumnsWithTypeAndName());

    forEachBackupBlock([&](const Block & block)
    {
        rebuilt_set->insertFromBlock(block.getColumnsWithTypeAndName());
    });
    rebuilt_set->finishInsert();

    std::lock_guard lock(mutex);
    set = std::move(rebuilt_set);
}

size_t StorageSet::getSize(ContextPtr) const
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    return current_set->getTotalRowCount();
}

std::optional<UInt64> StorageSet::totalRows(ContextPtr) const
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    return current_set->getTotalRowCount();
}

std::optional<UInt64> StorageSet::totalBytes(ContextPtr) const
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    return current_set->getTotalByteCount();
}

void StorageSet::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    if (disk->existsDirectory(path))
        disk->removeRecursive(path);
    else
        LOG_INFO(getLogger("StorageSet"), "Path {} is already removed from disk {}", path, disk->getName());

    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    Block header = metadata_snapshot->getSampleBlock();

    increment = 0;

    auto new_set = std::make_shared<Set>(SizeLimits(), 0, true);
    new_set->setHeader(header.getColumnsWithTypeAndName());
    {
        std::lock_guard lock(mutex);
        set = new_set;
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


void StorageSetOrJoinBase::restoreFromFile(const String & file_path, ContextPtr context)
{
    auto backup_buf = disk->readFile(file_path, getReadSettings());
    CompressedReadBuffer compressed_backup_buf(*backup_buf);
    NativeReader backup_stream(compressed_backup_buf, 0);

    ProfileInfo info;
    for (Block block = backup_stream.read(); !block.empty(); block = backup_stream.read())
    {
        info.update(block);
        insertBlock(block, context);
    }

    finishInsert();

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(getLogger("StorageSetOrJoinBase"), "Loaded from backup file {}. {} rows, {}. State has {} unique rows.",
        file_path, info.rows, ReadableSize(info.bytes), getSize(context));
}

void StorageSetOrJoinBase::forEachBackupBlock(const std::function<void(const Block &)> & callback, const String & exclude_file_name) const
{
    static const char * file_suffix = ".bin";
    static const auto file_suffix_size = strlen(".bin");

    using FilePriority = std::pair<UInt64, String>;
    std::priority_queue<FilePriority, std::vector<FilePriority>, std::greater<>> backup_files;
    for (auto dir_it{disk->iterateDirectory(path)}; dir_it->isValid(); dir_it->next())
    {
        const auto & name = dir_it->name();
        const auto & file_path = dir_it->path();
        if (name == exclude_file_name)
            continue;
        if (disk->existsFile(file_path) && endsWith(name, file_suffix) && disk->getFileSize(file_path) > 0)
            backup_files.push({parse<UInt64>(name.substr(0, name.size() - file_suffix_size)), file_path});
    }

    while (!backup_files.empty())
    {
        forEachBlockInBackupFile(backup_files.top().second, callback);
        backup_files.pop();
    }
}

void StorageSetOrJoinBase::forEachBlockInBackupFile(const String & file_path, const std::function<void(const Block &)> & callback) const
{
    auto backup_buf = disk->readFile(file_path, getReadSettings());
    CompressedReadBuffer compressed_backup_buf(*backup_buf);
    NativeReader backup_stream(compressed_backup_buf, 0);
    for (Block block = backup_stream.read(); !block.empty(); block = backup_stream.read())
        callback(block);
}


void StorageSetOrJoinBase::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    /// Rename directory with data. Use a directory move rather than `replaceFile`: on
    /// `DiskObjectStorage` `replaceFile` has file-only semantics and cannot rename a directory.
    disk->createDirectories(parentPath(new_path_to_table_data));
    disk->moveDirectory(path, new_path_to_table_data);

    path = new_path_to_table_data;
    renameInMemory(new_table_id);
}


void registerStorageSet(StorageFactory & factory);
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
    }, StorageFactory::StorageFeatures{ .supports_settings = true, .has_builtin_setting_fn = SetSettings::hasBuiltin, },
    Documentation{
        .description = R"DOCS_MD(
:::note
In ClickHouse Cloud, if your service was created with a version earlier than 25.4, you will need to set the compatibility to at least 25.4 using  `SET compatibility=25.4`.
:::

A data set that is always in RAM. It is intended for use on the right side of the `IN` operator (see the section "IN operators").

You can use `INSERT` to insert data in the table. New elements will be added to the data set, while duplicates will be ignored.
But you can't perform `SELECT` from the table. The only way to retrieve data is by using it in the right half of the `IN` operator.

Data is always located in RAM. For `INSERT`, the blocks of inserted data are also written to the directory of tables on the disk. When starting the server, this data is loaded to RAM. In other words, after restarting, the data remains in place.

For a rough server restart, the block of data on the disk might be lost or damaged. In the latter case, you may need to manually delete the file with damaged data.

### Limitations and settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

#### Persistent {#persistent}

Disables persistency for the Set and [Join](/reference/engines/table-engines/special/join) table engines.

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.
)DOCS_MD",
        .syntax = "ENGINE = Set",
        .related = {"Join"}});
}


}
