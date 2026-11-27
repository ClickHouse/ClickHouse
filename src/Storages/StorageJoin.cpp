#include <Storages/StorageJoin.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSet.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Disks/DiskFromAST.h>
#include <Interpreters/JoinUtils.h>
#include <IO/ReadHelpers.h>
#include <Formats/NativeWriter.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Poco/String.h>
#include <algorithm>
#include <filesystem>
#include <unordered_set>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool any_join_distinct_right_table_keys;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsOverflowMode join_overflow_mode;
    extern const SettingsBool join_use_nulls;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_rows_in_join;
    extern const SettingsUInt64 max_bytes_in_join;
}

namespace FailPoints
{
    extern const char storage_join_mutate_fail_after_moving_backup_aside[];
    extern const char storage_join_mutate_fail_before_commit[];
    extern const char storage_join_mutate_fail_putting_backup_back[];
    extern const char storage_join_mutate_fail_removing_superseded_backups[];
    extern const char storage_join_publish_fail_during_rollback[];
    extern const char storage_join_publish_fail_before_replay[];
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DEADLOCK_AVOIDED;
    extern const int FAULT_INJECTED;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_INITIALIZED;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

UInt64 parseBackupNumber(const std::string & file_name)
{
    static const auto file_suffix_size = strlen(".bin");
    return parse<UInt64>(file_name.substr(0, file_name.size() - file_suffix_size));
}

/// Returns the backups that a mutation moved into `aside_path`, in descending order of their numbers.
/// Both ways of emptying that directory (`putMovedBackupsBack` and `removeSupersededBackups`) go in this
/// order, so the smallest one leaves it last: `StorageJoin::recoverInterruptedMutation` decides whether
/// the mutation has committed by the presence of the smallest parked backup in the table directory, so
/// an interrupted emptying must never leave other parked backups behind without the smallest one.
std::vector<std::string> listMovedBackups(IDisk & disk, const String & aside_path)
{
    std::vector<std::string> file_names;
    disk.listFiles(aside_path, file_names);
    std::erase_if(file_names, [](const std::string & file_name) { return !file_name.ends_with(".bin"); });
    std::ranges::sort(file_names, std::greater{}, parseBackupNumber);
    return file_names;
}

/// Rolls back a mutation that has not committed: puts every backup it moved into `aside_path` back
/// into the table directory `path`, then removes `aside_path`. The smallest backup goes back last, so
/// it does not reappear in the table directory (which would mean the commit) while other backups of the
/// pre-mutation generation are still aside. The backups are taken from `aside_path` itself rather than
/// from a list kept by the mutation, so a backup that was moved but not recorded cannot be lost.
void putMovedBackupsBack(IDisk & disk, const String & aside_path, const String & path)
{
    for (const auto & file_name : listMovedBackups(disk, aside_path))
    {
        disk.replaceFile(fs::path(aside_path) / file_name, fs::path(path) / file_name);

        fiu_do_on(FailPoints::storage_join_mutate_fail_putting_backup_back,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault while putting a backup of a Join mutation back");
        });
    }
    disk.removeRecursive(aside_path);
}

/// Finishes a committed mutation: removes the superseded backups from `aside_path`, then `aside_path`
/// itself. The smallest backup is removed last: while any other one is still aside, the smallest one is
/// too, so an interrupted cleanup is still recognized as committed and is never rolled back to the old
/// generation. Once it is gone, nothing is left aside and there is nothing to restore.
void removeSupersededBackups(IDisk & disk, const String & aside_path)
{
    for (const auto & file_name : listMovedBackups(disk, aside_path))
    {
        disk.removeFile(fs::path(aside_path) / file_name);

        fiu_do_on(FailPoints::storage_join_mutate_fail_removing_superseded_backups,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault while removing the superseded backups of a Join mutation");
        });
    }
    disk.removeRecursive(aside_path);
}

}

StorageJoin::StorageJoin(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const Names & key_names_,
    bool use_nulls_,
    SizeLimits limits_,
    JoinKind kind_,
    JoinStrictness strictness_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool overwrite_,
    bool persistent_)
    : StorageSetOrJoinBase{disk_, relative_path_, table_id_, columns_, constraints_, comment, persistent_}
    , key_names(key_names_)
    , use_nulls(use_nulls_)
    , limits(limits_)
    , kind(kind_)
    , strictness(strictness_)
    , overwrite(overwrite_)
{
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    for (const auto & key : key_names)
        if (!metadata_snapshot->getColumns().hasPhysical(key))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Key column ({}) does not exist in table declaration.", key);

    table_join = std::make_shared<TableJoin>(limits, use_nulls, kind, strictness, key_names);
    /// Filled from one thread, then shared unchanged. A `SELECT` from the table can read only the serial map layout.
    join = HashJoin::create(table_join, std::make_shared<const Block>(getRightSampleBlock()), HashJoinBuildLayout::oneBucket(1), overwrite);
    recoverInterruptedMutation();
    restore();
    optimizeUnlocked();
}

void StorageJoin::recoverInterruptedMutation()
{
    const String aside_path = fs::path(path) / mutation_backups_dir;
    if (!disk->existsDirectory(aside_path))
        return;

    /// See `mutate`: the backups of the pre-mutation generation are moved here one by one, and
    /// the mutation commits by installing its consolidated backup under the smallest of their
    /// numbers. So the mutation has committed exactly when the table directory holds a backup
    /// with the smallest number found here: before the commit every moved backup is absent from
    /// the table directory, and backup numbers are never reused by inserts. This also holds while
    /// a failed mutation is putting the moved backups back, or while a committed one is removing the
    /// superseded backups, because both remove the smallest one from here last.
    const auto moved_backups = listMovedBackups(*disk, aside_path);
    const bool committed = !moved_backups.empty() && disk->existsFile(fs::path(path) / moved_backups.back());
    if (committed)
        removeSupersededBackups(*disk, aside_path);
    else
        putMovedBackupsBack(*disk, aside_path, path);

    LOG_INFO(getLogger("StorageJoin"), "Found an interrupted mutation of table {}, {} it", getStorageID().getNameForLogs(),
        committed ? "finished" : "rolled back");
}

void StorageJoin::checkLiveStateIsAvailable() const
{
    if (live_state_lost)
        throw Exception(ErrorCodes::NOT_INITIALIZED,
            "The in-memory state of table {} is unavailable: a failed INSERT could not be rolled back. "
            "It is rebuilt from the backup files by the next INSERT, ALTER DELETE, OPTIMIZE or on restart",
            getStorageID().getNameForLogs());
}

void StorageJoin::rebuildLiveStateIfLost(const String & exclude_file_name)
{
    if (!live_state_lost)
        return;

    /// A failed mutation whose rollback failed too can leave part of the committed backups parked
    /// in `tmp/mut_backups/`, where `buildFromBackups` would not see them.
    recoverInterruptedMutation();
    auto rebuilt_join = buildFromBackups(exclude_file_name);
    {
        /// Table data belongs to the server, not to the query releasing it.
        MemoryTrackerBlockerInThread table_data_not_charged_to_the_query;
        join = std::move(rebuilt_join);
    }
    live_state_lost = false;
    LOG_INFO(getLogger("StorageJoin"), "Rebuilt the in-memory state of table {} from the backup files", getStorageID().getNameForLogs());
}

String StorageJoin::getLockQueryId(const Context & context)
{
    String query_id = context.getInitialQueryId();
    if (query_id.empty())
        query_id = context.getCurrentQueryId();
    return query_id;
}

RWLockImpl::LockHolder StorageJoin::tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const
{
    const String query_id = context ? getLockQueryId(*context) : RWLockImpl::NO_QUERY;

    const std::chrono::milliseconds acquire_timeout
        = context ? std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()) : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return tryLockTimed(lock, type, query_id, Poco::Timespan(acquire_timeout.count() * 1000));
}

RWLockImpl::LockHolder StorageJoin::tryLockForCurrentQueryTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context)
{
    const String query_id = context ? getLockQueryId(*context) : RWLockImpl::NO_QUERY;

    const std::chrono::milliseconds acquire_timeout
        = context ? std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()) : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return lock->getLock(type, query_id, acquire_timeout, false);
}

SinkToStoragePtr StorageJoin::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    /// Reject `INSERT INTO j SELECT ... FROM j` before waiting on `mutate_mutex`: the source of such
    /// a query already holds the read lock, and a concurrent insert that holds `mutate_mutex` while
    /// publishing its backup waits for the write lock, so waiting here would be a lock cycle.
    checkInsertIsPossible(context);

    std::lock_guard mutate_lock(mutate_mutex);
    return StorageSetOrJoinBase::write(query, metadata_snapshot, context, /*async_insert=*/false);
}

bool StorageJoin::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & /* deduplicate_by_columns */,
    bool cleanup,
    ContextPtr context)
{

    if (partition)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition cannot be specified when optimizing table of type Join");

    if (final)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FINAL cannot be specified when optimizing table of type Join");

    if (deduplicate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DEDUPLICATE cannot be specified when optimizing table of type Join");

    if (cleanup)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEANUP cannot be specified when optimizing table of type Join");

    std::lock_guard mutate_lock(mutate_mutex);
    TableLockHolder lock_holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    rebuildLiveStateIfLost();
    optimizeUnlocked();
    return true;
}

void StorageJoin::optimizeUnlocked()
{
    size_t current_bytes = join->getTotalByteCount();
    size_t dummy = current_bytes;
    {
        /// Table data belongs to the server, not to the query releasing it.
        MemoryTrackerBlockerInThread table_data_not_charged_to_the_query;
        join->shrinkStoredBlocksToFit(dummy, true);
    }

    size_t optimized_bytes = join->getTotalByteCount();
    if (current_bytes > optimized_bytes)
        LOG_INFO(getLogger("StorageJoin"), "Optimized Join storage from {} to {} bytes", current_bytes, optimized_bytes);
}

void StorageJoin::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, TableExclusiveLockHolder &)
{
    std::lock_guard mutate_lock(mutate_mutex);
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    if (disk->existsDirectory(path))
        disk->removeRecursive(path);
    else
        LOG_INFO(getLogger("StorageJoin"), "Path {} is already removed from disk {}", path, disk->getName());

    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    increment = 0;
    discarded_backups.clear();
    {
        MemoryTrackerBlockerInThread table_data_not_charged_to_the_query;
        join = HashJoin::create(
            table_join, std::make_shared<const Block>(getRightSampleBlock()), HashJoinBuildLayout::oneBucket(1), overwrite);
    }
    live_state_lost = false;
}

void StorageJoin::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    for (const auto & command : commands)
        if (command.type != MutationCommand::DELETE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine Join supports only DELETE mutations");
}

void StorageJoin::mutate(const MutationCommands & commands, ContextPtr context)
{
    /// Firstly acquire lock for mutation, that locks changes of data.
    /// We cannot acquire rwlock here, because read lock is needed
    /// for execution of mutation interpreter.
    std::lock_guard mutate_lock(mutate_mutex);

    /// The mutation reads the live state, so bring it back first if a failed INSERT lost it.
    {
        TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);
        rebuildLiveStateIfLost();
    }

    constexpr auto tmp_backup_file_name = "tmp/mut.bin";
    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);

    auto backup_buf = disk->writeFile(path + tmp_backup_file_name);
    auto compressed_backup_buf = CompressedWriteBuffer(*backup_buf);
    auto backup_stream = NativeWriter(compressed_backup_buf, 0, std::make_shared<const Block>(metadata_snapshot->getSampleBlock()));

    auto new_data
        = HashJoin::create(table_join, std::make_shared<const Block>(getRightSampleBlock()), HashJoinBuildLayout::oneBucket(1), overwrite);

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
            Block block_to_insert = block;
            convertRightBlock(block_to_insert);
            new_data->addBlockToJoin(block_to_insert, block_to_insert.rows(), JoinBuildContext::serial());
            if (persistent)
                backup_stream.write(block);
        }
    }

    /// Make the new backup durable before anything is published: a failure while writing it must
    /// leave both the live state and the table directory exactly as they were.
    if (persistent)
    {
        backup_stream.flush();
        compressed_backup_buf.finalize();
        backup_buf->finalize();
    }
    else
    {
        compressed_backup_buf.cancel();
        backup_buf->cancel();
    }

    /// Now acquire exclusive lock and modify storage.
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    if (persistent)
    {
        /// Rewrite the table directory before publishing the mutated data in memory, so that a
        /// failure cannot leave queries seeing a state that a restart would not reproduce.
        ///
        /// The pre-mutation backups are moved aside into `tmp/mut_backups/` first, and the
        /// consolidated backup is installed with a single atomic replace only after all of them
        /// are out of the way. That replace is the commit point: before it, the directory holds no
        /// file of the new generation and every moved backup can be put back; after it, the
        /// directory holds only the new generation. A failure can never leave a mix of both
        /// generations behind, which would duplicate rows of `Join(ALL, ...)` or surface rows that
        /// a later backup of `Join(ANY, ...)` shadows. An interrupted rewrite (for example a server
        /// crash) is finished or rolled back on the next start by `recoverInterruptedMutation`.
        ///
        /// Backup names must stay unique for the lifetime of the table, and their numbers must keep
        /// the publication order: an insert that is still streaming its staged file will promote it
        /// after this mutation finishes, under a number taken from `increment` at that time. Reuse
        /// the smallest committed number for the consolidated backup -- `increment` is never
        /// rewound, so that number can no longer be handed out to a new insert -- and leave
        /// `increment` itself untouched. `recoverInterruptedMutation` relies on this choice to find
        /// the commit point.
        const String aside_path = fs::path(path) / mutation_backups_dir;
        recoverInterruptedMutation();

        std::optional<UInt64> consolidated_num;
        std::vector<std::string> committed_backups;
        std::vector<std::string> files;
        disk->listFiles(path, files);
        for (const auto & file_name: files)
        {
            /// A discarded backup is not part of the live state; its number must not be reused
            /// for the consolidated backup, which would then be skipped as well.
            if (file_name.ends_with(".bin") && !discarded_backups.contains(file_name))
            {
                UInt64 file_num = parseBackupNumber(file_name);
                if (!consolidated_num || file_num < *consolidated_num)
                    consolidated_num = file_num;
                committed_backups.push_back(file_name);
            }
        }

        if (!consolidated_num)
        {
            disk->removeFileIfExists(path + tmp_backup_file_name);
        }
        else
        {
            try
            {
                disk->createDirectories(aside_path);
                for (const auto & file_name : committed_backups)
                {
                    disk->replaceFile(path + file_name, fs::path(aside_path) / file_name);

                    fiu_do_on(FailPoints::storage_join_mutate_fail_after_moving_backup_aside,
                    {
                        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault after moving a backup of a Join mutation aside");
                    });
                }

                fiu_do_on(FailPoints::storage_join_mutate_fail_before_commit,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault before the commit of a Join mutation");
                });

                disk->replaceFile(path + tmp_backup_file_name, path + toString(*consolidated_num) + ".bin");
            }
            catch (...)
            {
                /// Nothing of the new generation is in the table directory, and the live state was
                /// never touched: putting the moved backups back restores the pre-mutation state.
                /// If that fails too, the directory is repaired by `recoverInterruptedMutation`
                /// before the next mutation and on the next start, and until then the live state
                /// still matches what the repair will restore.
                try
                {
                    if (disk->existsDirectory(aside_path))
                        putMovedBackupsBack(*disk, aside_path, path);
                }
                catch (...)
                {
                    tryLogCurrentException(
                        getLogger("StorageJoin"),
                        fmt::format("Cannot roll back the backup files of table {} after a failed mutation", getStorageID().getNameForLogs()));
                }
                throw;
            }

            /// The mutation is committed. Publish it before removing the old generation, so that a
            /// failure of the cleanup cannot leave the live state behind the directory.
            {
                MemoryTrackerBlockerInThread table_data_not_charged_to_the_query;
                join = std::move(new_data);
            }
            setCurrentQueryMemoryDriftExpected();
            live_state_lost = false;

            /// The superseded backups are outside of the set of restored files already, so a
            /// failure here does not affect the result of the committed mutation: it only leaves
            /// them behind until `recoverInterruptedMutation` removes them.
            try
            {
                removeSupersededBackups(*disk, aside_path);
            }
            catch (...)
            {
                tryLogCurrentException(
                    getLogger("StorageJoin"),
                    fmt::format("Cannot remove the superseded backup files of table {}, they will be removed by the next mutation or on restart",
                        getStorageID().getNameForLogs()));
            }
            return;
        }
    }

    {
        MemoryTrackerBlockerInThread table_data_not_charged_to_the_query;
        join = std::move(new_data);
    }
    setCurrentQueryMemoryDriftExpected();
    live_state_lost = false;
}

HashJoinPtr StorageJoin::getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, String query_id, std::chrono::milliseconds acquire_timeout, const Names & required_columns_names) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    if (!analyzed_join->sameStrictnessAndKind(strictness, kind))
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "Table '{}' has incompatible type of JOIN", getStorageID().getNameForLogs());

    if ((analyzed_join->forceNullableRight() && !use_nulls) ||
        (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Table {} needs the same join_use_nulls setting as present in LEFT or FULL JOIN",
            getStorageID().getNameForLogs());

    if (analyzed_join->getClauses().size() != 1)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "JOIN keys should match to the Join engine keys [{}]",
                        fmt::join(getKeyNames(), ", "));

    const auto & join_on = analyzed_join->getOnlyClause();
    if (join_on.on_filter_condition_left || join_on.on_filter_condition_right)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "ON section of JOIN with filter conditions is not implemented");

    /// The prebuilt join is reused as is (see reuseJoinedData below), so it cannot serve an
    /// expression the query derived: the names are unqualified, the saved block has a different
    /// layout and the maps variant may differ.
    if (analyzed_join->getMixedJoinExpression())
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "ON section of JOIN with a condition involving columns from both tables is not implemented for the Join table engine");

    const auto & key_names_right = join_on.key_names_right;
    const auto & key_names_left = join_on.key_names_left;
    if (key_names.size() != key_names_right.size() || key_names.size() != key_names_left.size())
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Number of keys in JOIN ON section ({}) doesn't match number of keys in Join engine ({})",
            key_names_right.size(), key_names.size());

    /* Resort left keys according to right keys order in StorageJoin
     * We can't change the order of keys in StorageJoin
     * because the hash table was already built with tuples serialized in the order of key_names.
     * If we try to use the same hash table with different order of keys,
     * then calculated hashes and the result of the comparison will be wrong.
     *
     * Example:
     * ```
     * CREATE TABLE t_right (a UInt32, b UInt32) ENGINE = Join(ALL, INNER, a, b);
     * SELECT * FROM t_left JOIN t_right ON t_left.y = t_right.b AND t_left.x = t_right.a;
     * ```
     * In that case right keys should still be (a, b), need to change the order of the left keys to (x, y).
     */
    Names left_key_names_resorted;
    for (const auto & key_name : key_names)
    {
        const auto & renamed_key = analyzed_join->renamedRightColumnNameWithAlias(key_name);
        /// find position of renamed_key in key_names_right
        auto it = std::find(key_names_right.begin(), key_names_right.end(), renamed_key);
        if (it == key_names_right.end())
            throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
                "Key '{}' not found in JOIN ON section. Join engine key{} '{}' have to be used",
                key_name, key_names.size() > 1 ? "s" : "", fmt::join(key_names, ", "));
        const size_t key_position = std::distance(key_names_right.begin(), it);
        left_key_names_resorted.push_back(key_names_left[key_position]);
    }

    /// Set qualified identifiers to original names (table.column -> column).
    /// It's required because storage join stores non-qualified names.
    /// Qualifies will be added by join implementation (TableJoin contains a rename mapping).
    analyzed_join->setRightKeys(key_names);
    analyzed_join->setLeftKeys(left_key_names_resorted);
    Block right_sample_block;
    for (const auto & name : required_columns_names)
        right_sample_block.insert(getRightSampleBlock().getByName(name));
    HashJoinPtr join_clone
        = HashJoin::create(analyzed_join, std::make_shared<const Block>(std::move(right_sample_block)), HashJoinBuildLayout::oneBucket(1));

    RWLockImpl::LockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, query_id, Poco::Timespan(acquire_timeout.count() * 1000));
    checkLiveStateIsAvailable();
    join_clone->setLock(holder);
    join_clone->reuseJoinedData(*join);

    return join_clone;
}

HashJoinPtr StorageJoin::getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, ContextPtr context, const Names & required_columns_names) const
{
    const String query_id = context ? getLockQueryId(*context) : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()) : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);

    return getJoinLocked(analyzed_join, query_id, acquire_timeout, required_columns_names);
}

void StorageJoin::insertBlock(const Block & block, ContextPtr context)
{
    Block block_to_insert = block;
    convertRightBlock(block_to_insert);
    TableLockHolder holder = tryLockForCurrentQueryTimedWithContext(rwlock, RWLockImpl::Write, context);

    /// Protection from `INSERT INTO test_table_join SELECT * FROM test_table_join`
    if (!holder)
        throw Exception(ErrorCodes::DEADLOCK_AVOIDED, "StorageJoin: cannot insert data because current query tries to read from this storage");

    join->addBlockToJoin(block_to_insert, block_to_insert.rows(), JoinBuildContext::serial());
}

void StorageJoin::checkInsertIsPossible(ContextPtr context) const
{
    /// Protection from `INSERT INTO test_table_join SELECT * FROM test_table_join`: the write lock
    /// cannot be taken while the same query holds a read lock on this table.
    if (!tryLockForCurrentQueryTimedWithContext(rwlock, RWLockImpl::Write, context))
        throw Exception(
            ErrorCodes::DEADLOCK_AVOIDED, "StorageJoin: cannot insert data because current query tries to read from this storage");
}

HashJoinPtr StorageJoin::buildFromBackups(const String & exclude_file_name) const
{
    auto rebuilt_join
        = HashJoin::create(table_join, std::make_shared<const Block>(getRightSampleBlock()), HashJoinBuildLayout::oneBucket(1), overwrite);
    forEachBackupBlock([&](const Block & block)
    {
        Block block_to_insert = block;
        convertRightBlock(block_to_insert);
        rebuilt_join->addBlockToJoin(block_to_insert, block_to_insert.rows(), JoinBuildContext::serial());
    }, exclude_file_name);
    return rebuilt_join;
}

void StorageJoin::publishBackup(const String & backup_file_path, ContextPtr context)
{
    /// Hold the write lock across the whole replay: a reader must never observe a partially
    /// replayed backup, and the rollback of a failed replay must not have to reacquire the lock
    /// after the partial state was already exposed -- a long-running reader could block that
    /// reacquisition until `lock_acquire_timeout`, leaving the rows of the failed insert visible
    /// until restart.
    TableLockHolder holder = tryLockForCurrentQueryTimedWithContext(rwlock, RWLockImpl::Write, context);
    if (!holder)
        throw Exception(
            ErrorCodes::DEADLOCK_AVOIDED, "StorageJoin: cannot insert data because current query tries to read from this storage");

    rebuildLiveStateIfLost(/*exclude_file_name=*/ fs::path(backup_file_path).filename());

    fiu_do_on(FailPoints::storage_join_publish_fail_before_replay,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault before replaying the backup of an INSERT into a Join");
    });

    try
    {
        forEachBlockInBackupFile(backup_file_path, [this](const Block & block)
        {
            Block block_to_insert = block;
            convertRightBlock(block_to_insert);
            join->addBlockToJoin(block_to_insert, block_to_insert.rows(), JoinBuildContext::serial());
        });
    }
    catch (...)
    {
        /// The write lock is still held, so the partially updated state is replaced before any
        /// reader can see it. The rebuild skips the backup of the failed insert, which the caller
        /// discards before leaving the publish critical section.

        /// The rollback builds a second copy of the table, so it can fail as well (for example,
        /// on memory limits). The partially updated state must not stay visible then: mark it as
        /// lost, so that readers get an exception instead of the rows of the failed insert, until
        /// the next operation under the write lock rebuilds it from the backup files.
        live_state_lost = true;
        try
        {
            fiu_do_on(FailPoints::storage_join_publish_fail_during_rollback,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault during the rollback of a failed INSERT into a Join");
            });
            /// The live state stays correct while a failed mutation leaves part of the committed
            /// backups parked in `tmp/mut_backups/`, but a rebuild from the table directory alone
            /// would lose their rows. The write lock excludes a running mutation, so the parked
            /// backups can only belong to an interrupted one.
            recoverInterruptedMutation();
            auto rebuilt_join = buildFromBackups(/*exclude_file_name=*/ fs::path(backup_file_path).filename());
            {
                MemoryTrackerBlockerInThread table_data_not_charged_to_the_query;
                join = std::move(rebuilt_join);
            }
            live_state_lost = false;
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("StorageJoin"),
                fmt::format("Cannot restore the in-memory state of table {} after a failed INSERT", getStorageID().getNameForLogs()));
        }
        throw;
    }
}

size_t StorageJoin::getSize(ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    checkLiveStateIsAvailable();
    return join->getTotalRowCount();
}

namespace
{

/// Whether a read of the table returns every stored row of a key rather than one row per key.
constexpr bool readsAllRowsOfKey(JoinKind kind, JoinStrictness strictness)
{
    return strictness == JoinStrictness::All || (kind == JoinKind::Right && strictness != JoinStrictness::RightAny);
}

}

std::optional<UInt64> StorageJoin::totalRows(ContextPtr query_context) const
{
    const auto & settings = query_context->getSettingsRef();
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings[Setting::lock_acquire_timeout]);
    if (live_state_lost)
        return {};
    if (!readsAllRowsOfKey(join->getKind(), join->getStrictness()))
        return join->getTotalRowCount();

    /// getTotalRowCount counts keys, so the rows of each key are summed here.
    const auto data = join->getJoinedData();
    const HashJoinTable & table = *data->tables.front();
    if (!table.holds<HashJoin::MapsAll>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageJoin keeps every row of a key in a map that is not MapsAll");

    UInt64 rows = 0;
    auto count_rows = [&](const auto &, const RowRefList & mapped)
    {
        rows += mapped.rows();
        return size_t(0);
    };
    HashJoinTable::Cursor cursor;
    table.forEachCell<HashJoin::MapsAll>(cursor, std::numeric_limits<size_t>::max(), count_rows);
    return rows;
}

std::optional<UInt64> StorageJoin::totalBytes(ContextPtr query_context) const
{
    const auto & settings = query_context->getSettingsRef();
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings[Setting::lock_acquire_timeout]);
    if (live_state_lost)
        return {};
    return join->getTotalByteCount();
}

DataTypePtr StorageJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null, ContextPtr context) const
{
    /// A failed persistent `INSERT`, a mutation or `TRUNCATE` replaces `join` under the write lock and
    /// may destroy the old `HashJoin`, so reading it without the lock is a use-after-free. The checked
    /// key and column types do not depend on the data, so a lost live state does not matter here.
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return join->joinGetCheckAndGetReturnType(data_types, column_name, or_null);
}

ColumnWithTypeAndName StorageJoin::joinGet(const Block & block, const Block & block_with_columns_to_add, ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    checkLiveStateIsAvailable();
    return join->joinGet(block, block_with_columns_to_add);
}

void StorageJoin::convertRightBlock(Block & block) const
{
    bool need_covert = use_nulls && isLeftOrFull(kind);
    if (!need_covert)
        return;

    for (auto & col : block)
        JoinCommon::convertColumnToNullable(col);
}

void registerStorageJoin(StorageFactory & factory);
void registerStorageJoin(StorageFactory & factory)
{
    auto has_builtin_fn = [](std::string_view name)
    {
        static const std::unordered_set<std::string_view> valid_settings
            = {"join_use_nulls",
               "max_rows_in_join",
               "max_bytes_in_join",
               "join_overflow_mode",
               "join_any_take_last_row",
               "any_join_distinct_right_table_keys",
               "disk",
               "persistent"};
        return valid_settings.contains(name);
    };

    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        /// Join(ANY, LEFT, k1, k2, ...)

        ASTs & engine_args = args.engine_args;

        const auto & settings = args.getContext()->getSettingsRef();

        auto join_use_nulls = settings[Setting::join_use_nulls];
        auto max_rows_in_join = settings[Setting::max_rows_in_join];
        auto max_bytes_in_join = settings[Setting::max_bytes_in_join];
        auto join_overflow_mode = settings[Setting::join_overflow_mode];
        auto join_any_take_last_row = settings[Setting::join_any_take_last_row];
        auto old_any_join = settings[Setting::any_join_distinct_right_table_keys];
        bool persistent = true;
        String disk_name = "default";

        if (args.storage_def && args.storage_def->settings)
        {
            for (const auto & setting : args.storage_def->settings->changes)
            {
                /// These settings are read here rather than applied to a `BaseSettings`, so there is no
                /// settings schema to check the value-less form `SETTINGS name` against - it stands for
                /// `name = true` and is only meaningful for the Bool ones. Without this check
                /// `SETTINGS max_rows_in_join` would silently become `max_rows_in_join = 1`.
                if (setting.shorthand && setting.name != "join_use_nulls" && setting.name != "join_any_take_last_row"
                    && setting.name != "any_join_distinct_right_table_keys" && setting.name != "persistent")
                    BaseSettingsHelpers::throwValuelessSettingIsNotBool(setting.name);

                if (setting.name == "join_use_nulls")
                    join_use_nulls = setting.value;
                else if (setting.name == "max_rows_in_join")
                    max_rows_in_join = setting.value;
                else if (setting.name == "max_bytes_in_join")
                    max_bytes_in_join = setting.value;
                else if (setting.name == "join_overflow_mode")
                    join_overflow_mode = setting.value;
                else if (setting.name == "join_any_take_last_row")
                    join_any_take_last_row = setting.value;
                else if (setting.name == "any_join_distinct_right_table_keys")
                    old_any_join = setting.value;
                else if (setting.name == "disk")
                {
                    DiskFromAST::ensureDiskIsNotInline(setting.value);
                    disk_name = setting.value.safeGet<String>();
                }
                else if (setting.name == "persistent")
                {
                    persistent = setting.value.safeGet<bool>();
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting {} for storage {}", setting.name, args.engine_name);
            }
        }

        DiskPtr disk = args.getDisk(disk_name);

        if (engine_args.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Join requires at least 3 parameters: "
                            "Join(ANY|ALL|SEMI|ANTI, LEFT|INNER|RIGHT, keys...).");

        JoinStrictness strictness = JoinStrictness::Unspecified;
        JoinKind kind = JoinKind::Comma;

        if (auto opt_strictness_id = tryGetIdentifierName(engine_args[0]))
        {
            const String strictness_str = Poco::toLower(*opt_strictness_id);

            if (strictness_str == "any")
            {
                if (old_any_join)
                    strictness = JoinStrictness::RightAny;
                else
                    strictness = JoinStrictness::Any;
            }
            else if (strictness_str == "all")
                strictness = JoinStrictness::All;
            else if (strictness_str == "semi")
                strictness = JoinStrictness::Semi;
            else if (strictness_str == "anti")
                strictness = JoinStrictness::Anti;
        }

        if (strictness == JoinStrictness::Unspecified)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First parameter of storage Join must be ANY or ALL or SEMI or ANTI (without quotes).");

        if (auto opt_kind_id = tryGetIdentifierName(engine_args[1]))
        {
            const String kind_str = Poco::toLower(*opt_kind_id);

            if (kind_str == "left")
                kind = JoinKind::Left;
            else if (kind_str == "inner")
                kind = JoinKind::Inner;
            else if (kind_str == "right")
                kind = JoinKind::Right;
            else if (kind_str == "full")
            {
                if (strictness == JoinStrictness::Any)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ANY FULL JOINs are not implemented");
                kind = JoinKind::Full;
            }
        }

        if ((strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti) && (kind != JoinKind::Left && kind != JoinKind::Right))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, " SEMI|ANTI JOIN should be LEFT or RIGHT");

        if (kind == JoinKind::Comma)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter of storage Join must be LEFT or INNER or RIGHT or FULL (without quotes).");

        Names key_names;
        key_names.reserve(engine_args.size() - 2);
        for (size_t i = 2, size = engine_args.size(); i < size; ++i)
        {
            auto opt_key = tryGetIdentifierName(engine_args[i]);
            if (!opt_key)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter №{} of storage Join don't look like column name.", i + 1);

            key_names.push_back(*opt_key);
        }

        return std::make_shared<StorageJoin>(
            disk,
            args.relative_data_path,
            args.table_id,
            key_names,
            join_use_nulls,
            SizeLimits{max_rows_in_join, max_bytes_in_join, join_overflow_mode},
            kind,
            strictness,
            args.columns,
            args.constraints,
            args.comment,
            join_any_take_last_row,
            persistent);
    };

    factory.registerStorage(
        "Join",
        creator_fn,
        SecretArgumentsSpec{},
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .has_builtin_setting_fn = has_builtin_fn,
            .stores_data_on_disk = true,
            .supports_temporary = true,
        },
        Documentation{
            .description = R"DOCS_MD(
Optional prepared data structure for usage in [JOIN](/reference/statements/select/join) operations.

<Note>
In ClickHouse Cloud, if your service was created with a version earlier than 25.4, you will need to set the compatibility to at least 25.4 using  `SET compatibility=25.4`.
</Note>

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

See the detailed description of the [CREATE TABLE](/reference/statements/create/table) query.

## Engine parameters {#engine-parameters}

### `join_strictness` {#join_strictness}

`join_strictness` – [JOIN strictness](/reference/statements/select/join#supported-types-of-join).

### `join_type` {#join_type}

`join_type` – [JOIN type](/reference/statements/select/join#supported-types-of-join).

### Key columns {#key-columns}

`k1[, k2, ...]` – Key columns from the `USING` clause that the `JOIN` operation is made with.

Enter `join_strictness` and `join_type` parameters without quotes, for example, `Join(ANY, LEFT, col1)`. They must match the `JOIN` operation that the table will be used for. If the parameters do not match, ClickHouse does not throw an exception and may return incorrect data.

## Specifics and recommendations {#specifics-and-recommendations}

### Data storage {#data-storage}

`Join` table data is always located in the RAM. When inserting rows into a table, ClickHouse writes data blocks to the directory on the disk so that they can be restored when the server restarts.

If the server restarts incorrectly, the data block on the disk might get lost or damaged. In this case, you may need to manually delete the file with damaged data.

### Selecting and Inserting Data {#selecting-and-inserting-data}

You can use `INSERT` queries to add data to the `Join`-engine tables. If the table was created with the `ANY` strictness, data for duplicate keys are ignored. With the `ALL` strictness, all rows are added.

Main use-cases for `Join`-engine tables are following:

- Place the table to the right side in a `JOIN` clause.
- Call the [joinGet](/reference/functions/regular-functions/other-functions#joinGet) function, which lets you extract data from the table the same way as from a dictionary.

### Deleting data {#deleting-data}

`ALTER DELETE` queries for `Join`-engine tables are implemented as [mutations](/reference/statements/alter/index#mutations). `DELETE` mutation reads filtered data and overwrites data of memory and disk.

### Limitations and settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

#### `join_use_nulls` {#join_use_nulls}

[join_use_nulls](/reference/settings/session-settings/join#join_use_nulls)

#### `max_rows_in_join` {#max_rows_in_join}

[max_rows_in_join](/reference/settings/session-settings/max-rows#max_rows_in_join)

#### `max_bytes_in_join` {#max_bytes_in_join}

[max_bytes_in_join](/reference/settings/session-settings/max-bytes#max_bytes_in_join)

#### `join_overflow_mode` {#join_overflow_mode}

[join_overflow_mode](/reference/settings/session-settings/join#join_overflow_mode)

#### `join_any_take_last_row` {#join_any_take_last_row}

[join_any_take_last_row](/reference/settings/session-settings/join#join_any_take_last_row)
#### `join_use_nulls` {#join_use_nulls-1}

#### Persistent {#persistent}

Disables persistency for the Join and [Set](/reference/engines/table-engines/special/set) table engines.

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.

The `Join`-engine tables can't be used in `GLOBAL JOIN` operations.

A [row policy](/reference/statements/create/row-policy) on a `Join`-engine table filters a plain `SELECT` from it, but a `JOIN` or `joinGet` reads the prepared hash table as is and cannot filter its rows, so while a policy applies to the table such queries fail with `ACCESS_DENIED`.

The `Join`-engine allows to specify [join_use_nulls](/reference/settings/session-settings/join#join_use_nulls) setting in the `CREATE TABLE` statement. [SELECT](/reference/statements/select/index) query should have the same `join_use_nulls` value.

## Usage examples {#example}

Creating the left-side table:

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11), (2,12), (3,13);
```

Creating the right-side `Join` table:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21), (1,22), (3,23);
```

Joining the tables:

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

As an alternative, you can retrieve data from the `Join` table, specifying the join key value:

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

Deleting a row from the `Join` table:

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```
)DOCS_MD",
            .syntax = "ENGINE = Join(join_strictness, join_type, k1[, k2, ...])",
            .related = {"Set"}});
}

class JoinSource final : public ISource
{
public:
    JoinSource(HashJoinPtr join_, TableLockHolder lock_holder_, UInt64 max_block_size_, SharedHeader sample_block_)
        : ISource(sample_block_)
        , join(join_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
    {
        const auto & table_join = join->getTableJoin();
        if (!table_join.oneDisjunct())
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin does not support OR for keys in JOIN ON section");

        column_indices.resize(sample_block->columns());

        auto & saved_block = join->getJoinedData()->sample_block;
        std::unordered_map<String, size_t> key_output_positions;

        for (size_t i = 0; i < sample_block->columns(); ++i)
        {
            const auto & [_, type, name] = sample_block->getByPosition(i);
            if (join->right_table_keys.has(name))
            {
                key_output_positions.emplace(name, i);
                const auto & column = join->right_table_keys.getByName(name);
                restored_block.insert(column);
            }
            else
            {
                size_t pos = saved_block.getPositionByName(name);
                column_indices[i] = pos;

                const auto & column = saved_block.getByPosition(pos);
                restored_block.insert(column);
            }
        }

        /// The output position of each key, in clause order. It comes from the clause and not from
        /// `right_table_keys`, which deduplicates a repeated key name while the packed key does not.
        const auto & key_names_right = table_join.getOnlyClause().key_names_right;

        is_key_column.assign(sample_block->columns(), false);
        for (const auto & key_name : key_names_right)
        {
            auto & output = key_outputs.emplace_back();
            if (auto it = key_output_positions.find(key_name); it != key_output_positions.end())
            {
                output = it->second;
                is_key_column[it->second] = true;
            }
        }
    }

    String getName() const override { return "Join"; }

protected:
    Chunk generate() override
    {
        if (join->data->build_streams.front().columns.empty())
            return {};

        Chunk chunk;
        if (!dispatchJoinShape(
                join->kind,
                join->strictness,
                join->getMapsKind(),
                [&]<JoinKind kind, JoinStrictness strictness, typename Maps>()
                {
                    /// `StorageJoin` reads the right rows back out of the maps, so it never stores them
                    /// in a map that keeps none.
                    if constexpr (SetJoinMaps<Maps>)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageJoin cannot read rows from a set map");
                    else
                        chunk = createChunk<kind, strictness, Maps>();
                }))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness");
        return chunk;
    }

private:
    HashJoinPtr join;
    TableLockHolder lock_holder;

    UInt64 max_block_size;
    SharedHeader sample_block;
    Block restored_block; /// sample_block with parent column types

    ColumnNumbers column_indices;
    /// One entry per engine key argument, in clause order: the position of the key in the output, if it is selected.
    std::vector<std::optional<size_t>> key_outputs;
    /// Indexed by output column position, so the fill loops test a column in constant time.
    std::vector<bool> is_key_column;

    HashJoinTable::Cursor position;

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
    Chunk createChunk()
    {
        MutableColumns mut_columns = restored_block.cloneEmpty().mutateColumns();

        const size_t rows_added = fillColumns<KIND, STRICTNESS, Maps>(mut_columns);
        if (!rows_added)
            return {};

        Columns columns;
        columns.reserve(mut_columns.size());
        for (auto & col : mut_columns)
            columns.emplace_back(std::move(col));

        /// Correct nullability and LowCardinality types
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & src = restored_block.getByPosition(i);
            const auto & dst = sample_block->getByPosition(i);

            if (!src.type->equals(*dst.type))
            {
                auto arg = src;
                arg.column = std::move(columns[i]);
                columns[i] = castColumn(arg, dst.type);
            }
        }

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
    size_t fillColumns(MutableColumns & columns)
    {
        const StoredBlock * const * stored_columns = join->getJoinedData()->stored_columns_index->blocksData();

        return join->data->tables.front()->readCells<Maps>(
            position,
            max_block_size,
            key_outputs,
            columns,
            [&]([[maybe_unused]] const auto & mapped) -> size_t
            {
                if constexpr (STRICTNESS == JoinStrictness::Asof)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This JOIN is not implemented yet");
                /// RIGHT ANTI has a map with one row per key only for `EXCEPT DISTINCT`, never in a `Join` table.
                else if constexpr (readsAllRowsOfKey(KIND, STRICTNESS) && std::is_same_v<std::decay_t<decltype(mapped)>, RowRefList>)
                    return fillAll(columns, column_indices, is_key_column, mapped, stored_columns);
                else
                    return fillOne(columns, column_indices, is_key_column, mapped, stored_columns);
            });
    }

    template <typename Mapped>
    static size_t fillOne(MutableColumns & columns, const ColumnNumbers & column_indices, const std::vector<bool> & is_key_column,
                          const Mapped & mapped, const StoredBlock * const * stored_columns)
    {
        /// The mapped value of MapsOne is a single encoded ref; the mapped value of MapsAll
        /// (RightAny under preferUseMapsAll) is a tagged ref list whose first element is taken.
        const UInt64 ref_word = firstRefWord(mapped);
        const StoredBlock * block = stored_columns[refWordBlockNo(ref_word)];
        for (size_t j = 0; j < columns.size(); ++j)
            if (!is_key_column[j])
                columns[j]->insertFrom(*block->columns[column_indices[j]], refWordRowNo(ref_word));
        return 1;
    }

    template <typename Mapped>
    static size_t fillAll(MutableColumns & columns, const ColumnNumbers & column_indices, const std::vector<bool> & is_key_column,
                          const Mapped & mapped, const StoredBlock * const * stored_columns)
    {
        size_t rows_added = 0;
        for (auto ref_it = mapped.begin(); ref_it.ok(); ++ref_it)
        {
            const UInt64 ref_word = *ref_it;
            const StoredBlock * block = stored_columns[refWordBlockNo(ref_word)];
            for (size_t j = 0; j < columns.size(); ++j)
                if (!is_key_column[j])
                    columns[j]->insertFrom(*block->columns[column_indices[j]], refWordRowNo(ref_word));
            ++rows_added;
        }
        return rows_added;
    }
};


// TODO: multiple stream read and index read
Pipe StorageJoin::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    auto source_sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    checkLiveStateIsAvailable();
    return Pipe(std::make_shared<JoinSource>(join, std::move(holder), max_block_size, source_sample_block));
}

}
