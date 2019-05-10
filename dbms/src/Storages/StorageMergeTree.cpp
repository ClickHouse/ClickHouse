#include <Databases/IDatabase.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitors.h>
#include <Common/ThreadPool.h>
#include <Common/localBackup.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/AlterCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeList.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int INCOMPATIBLE_COLUMNS;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}


StorageMergeTree::StorageMergeTree(
    const String & path_,
    const String & database_name_,
    const String & table_name_,
    const ColumnsDescription & columns_,
    const IndicesDescription & indices_,
    bool attach,
    Context & context_,
    const String & date_column_name,
    const ASTPtr & partition_by_ast_,
    const ASTPtr & order_by_ast_,
    const ASTPtr & primary_key_ast_,
    const ASTPtr & sample_by_ast_, /// nullptr, if sampling is not supported.
    const ASTPtr & ttl_table_ast_,
    const MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag)
        : MergeTreeData(database_name_, table_name_,
            path_ + escapeForFileName(table_name_) + '/',
            columns_, indices_,
            context_, date_column_name, partition_by_ast_, order_by_ast_, primary_key_ast_,
            sample_by_ast_, ttl_table_ast_, merging_params_,
            settings_, false, attach),
        path(path_),
        background_pool(context_.getBackgroundPool()),
        reader(*this), writer(*this), merger_mutator(*this, global_context.getBackgroundPool())
{
    if (path.empty())
        throw Exception("MergeTree require data path", ErrorCodes::INCORRECT_FILE_NAME);

    loadDataParts(has_force_restore_data_flag);

    if (!attach && !getDataParts().empty())
        throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

    increment.set(getMaxBlockNumber());

    loadMutations();
}


void StorageMergeTree::startup()
{
    clearOldPartsFromFilesystem();

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    clearOldTemporaryDirectories(0);

    /// NOTE background task will also do the above cleanups periodically.
    time_after_previous_cleanup.restart();
    background_task_handle = background_pool.addTask([this] { return backgroundTask(); });
}


void StorageMergeTree::shutdown()
{
    if (shutdown_called)
        return;
    shutdown_called = true;
    merger_mutator.actions_blocker.cancelForever();
    if (background_task_handle)
        background_pool.removeTask(background_task_handle);
}


StorageMergeTree::~StorageMergeTree()
{
    shutdown();
}

BlockInputStreams StorageMergeTree::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned num_streams)
{
    return reader.read(column_names, query_info, context, max_block_size, num_streams);
}

BlockOutputStreamPtr StorageMergeTree::write(const ASTPtr & /*query*/, const Context & context)
{
    return std::make_shared<MergeTreeBlockOutputStream>(*this, context.getSettingsRef().max_partitions_per_insert_block);
}

void StorageMergeTree::checkTableCanBeDropped() const
{
    const_cast<StorageMergeTree &>(*this).recalculateColumnSizes();
    global_context.checkTableCanBeDropped(database_name, table_name, getTotalActiveSizeInBytes());
}

void StorageMergeTree::checkPartitionCanBeDropped(const ASTPtr & partition)
{
    const_cast<StorageMergeTree &>(*this).recalculateColumnSizes();

    const String partition_id = getPartitionIDFromQuery(partition, global_context);
    auto parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
    {
        partition_size += part->bytes_on_disk;
    }
    global_context.checkPartitionCanBeDropped(database_name, table_name, partition_size);
}

void StorageMergeTree::drop()
{
    shutdown();
    dropAllData();
}

void StorageMergeTree::truncate(const ASTPtr &, const Context &)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.actions_blocker.cancel();

        /// NOTE: It's assumed that this method is called under lockForAlter.

        auto parts_to_remove = getDataPartsVector();
        removePartsFromWorkingSet(parts_to_remove, true);

        LOG_INFO(log, "Removed " << parts_to_remove.size() << " parts.");
    }

    clearOldPartsFromFilesystem();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

    setPath(new_full_path);

    path = new_path_to_db;
    table_name = new_table_name;
    full_path = new_full_path;

    /// NOTE: Logger names are not updated.
}


std::vector<MergeTreeData::AlterDataPartTransactionPtr> StorageMergeTree::prepareAlterTransactions(
    const ColumnsDescription & new_columns, const IndicesDescription & new_indices, const Context & context)
{
    auto parts = getDataParts({MergeTreeDataPartState::PreCommitted,
                                    MergeTreeDataPartState::Committed,
                                    MergeTreeDataPartState::Outdated});
    std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;
    transactions.reserve(parts.size());

    const auto & columns_for_parts = new_columns.getAllPhysical();

    const Settings & settings = context.getSettingsRef();
    size_t thread_pool_size = std::min<size_t>(parts.size(), settings.max_alter_threads);
    ThreadPool thread_pool(thread_pool_size);


    for (const auto & part : parts)
    {
        transactions.push_back(std::make_unique<MergeTreeData::AlterDataPartTransaction>(part));

        thread_pool.schedule(
            [this, & transaction = transactions.back(), & columns_for_parts, & new_indices = new_indices.indices]
            {
                this->alterDataPart(columns_for_parts, new_indices, false, transaction);
            }
        );
    }

    thread_pool.wait();

    auto erase_pos = std::remove_if(transactions.begin(), transactions.end(),
        [](const MergeTreeData::AlterDataPartTransactionPtr & transaction)
        {
            return !transaction->isValid();
        }
    );
    transactions.erase(erase_pos, transactions.end());

    return transactions;
}

void StorageMergeTree::alter(
    const AlterCommands & params,
    const String & current_database_name,
    const String & current_table_name,
    const Context & context,
    TableStructureWriteLockHolder & table_lock_holder)
{
    if (!params.isMutable())
    {
        lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());
        auto new_columns = getColumns();
        auto new_indices = getIndices();
        params.apply(new_columns);
        context.getDatabase(current_database_name)->alterTable(context, current_table_name, new_columns, new_indices, {});
        setColumns(std::move(new_columns));
        return;
    }

    /// NOTE: Here, as in ReplicatedMergeTree, you can do ALTER which does not block the writing of data for a long time.
    auto merge_blocker = merger_mutator.actions_blocker.cancel();

    lockNewDataStructureExclusively(table_lock_holder, context.getCurrentQueryId());

    checkAlter(params, context);

    auto new_columns = getColumns();
    auto new_indices = getIndices();
    ASTPtr new_order_by_ast = order_by_ast;
    ASTPtr new_primary_key_ast = primary_key_ast;
    ASTPtr new_ttl_table_ast = ttl_table_ast;
    params.apply(new_columns, new_indices, new_order_by_ast, new_primary_key_ast, new_ttl_table_ast);

    auto transactions = prepareAlterTransactions(new_columns, new_indices, context);

    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());

    IDatabase::ASTModifier storage_modifier = [&] (IAST & ast)
    {
        auto & storage_ast = ast.as<ASTStorage &>();

        if (new_order_by_ast.get() != order_by_ast.get())
            storage_ast.set(storage_ast.order_by, new_order_by_ast);

        if (new_primary_key_ast.get() != primary_key_ast.get())
            storage_ast.set(storage_ast.primary_key, new_primary_key_ast);

        if (new_ttl_table_ast.get() != ttl_table_ast.get())
            storage_ast.set(storage_ast.ttl_table, new_ttl_table_ast);
    };

    context.getDatabase(current_database_name)->alterTable(context, current_table_name, new_columns, new_indices, storage_modifier);

    /// Reinitialize primary key because primary key column types might have changed.
    setPrimaryKeyIndicesAndColumns(new_order_by_ast, new_primary_key_ast, new_columns, new_indices);

    setTTLExpressions(new_columns.getColumnTTLs(), new_ttl_table_ast);

    for (auto & transaction : transactions)
    {
        transaction->commit();
        transaction.reset();
    }

    /// Columns sizes could be changed
    recalculateColumnSizes();
}


/// While exists, marks parts as 'currently_merging' and reserves free space on filesystem.
struct CurrentlyMergingPartsTagger
{
    FutureMergedMutatedPart future_part;
    DiskSpaceMonitor::ReservationPtr reserved_space;

    bool is_successful = false;
    String exception_message;

    StorageMergeTree & storage;

public:
    CurrentlyMergingPartsTagger(const FutureMergedMutatedPart & future_part_, size_t total_size, StorageMergeTree & storage_)
        : future_part(future_part_), storage(storage_)
    {
        /// Assume mutex is already locked, because this method is called from mergeTask.
        reserved_space = DiskSpaceMonitor::reserve(storage.full_path, total_size); /// May throw.

        for (const auto & part : future_part.parts)
        {
            if (storage.currently_merging.count(part))
                throw Exception("Tagging alreagy tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
        }
        storage.currently_merging.insert(future_part.parts.begin(), future_part.parts.end());
    }

    ~CurrentlyMergingPartsTagger()
    {
        std::lock_guard lock(storage.currently_merging_mutex);

        for (const auto & part : future_part.parts)
        {
            if (!storage.currently_merging.count(part))
                std::terminate();
            storage.currently_merging.erase(part);
        }

        /// Update the information about failed parts in the system.mutations table.

        Int64 sources_data_version = future_part.parts.at(0)->info.getDataVersion();
        Int64 result_data_version = future_part.part_info.getDataVersion();
        auto mutations_begin_it = storage.current_mutations_by_version.end();
        auto mutations_end_it = storage.current_mutations_by_version.end();
        if (sources_data_version != result_data_version)
        {
            mutations_begin_it = storage.current_mutations_by_version.upper_bound(sources_data_version);
            mutations_end_it = storage.current_mutations_by_version.upper_bound(result_data_version);
        }

        for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
        {
            MergeTreeMutationEntry & entry = it->second;
            if (is_successful)
            {
                if (!entry.latest_failed_part.empty() && future_part.part_info.contains(entry.latest_failed_part_info))
                {
                    entry.latest_failed_part.clear();
                    entry.latest_failed_part_info = MergeTreePartInfo();
                    entry.latest_fail_time = 0;
                    entry.latest_fail_reason.clear();
                }
            }
            else
            {
                entry.latest_failed_part = future_part.parts.at(0)->name;
                entry.latest_failed_part_info = future_part.parts.at(0)->info;
                entry.latest_fail_time = time(nullptr);
                entry.latest_fail_reason = exception_message;
            }
        }
    }
};


void StorageMergeTree::mutate(const MutationCommands & commands, const Context &)
{
    MergeTreeMutationEntry entry(commands, full_path, insert_increment.get());
    String file_name;
    {
        std::lock_guard lock(currently_merging_mutex);

        Int64 version = increment.get();
        entry.commit(version);
        file_name = entry.file_name;
        auto insertion = current_mutations_by_id.emplace(file_name, std::move(entry));
        current_mutations_by_version.emplace(version, insertion.first->second);
    }

    LOG_INFO(log, "Added mutation: " << file_name);
    background_task_handle->wake();
}

std::vector<MergeTreeMutationStatus> StorageMergeTree::getMutationsStatus() const
{
    std::lock_guard lock(currently_merging_mutex);

    std::vector<Int64> part_data_versions;
    auto data_parts = getDataPartsVector();
    part_data_versions.reserve(data_parts.size());
    for (const auto & part : data_parts)
        part_data_versions.push_back(part->info.getDataVersion());
    std::sort(part_data_versions.begin(), part_data_versions.end());

    std::vector<MergeTreeMutationStatus> result;
    for (const auto & kv : current_mutations_by_version)
    {
        Int64 mutation_version = kv.first;
        const MergeTreeMutationEntry & entry = kv.second;

        auto versions_it = std::lower_bound(
            part_data_versions.begin(), part_data_versions.end(), mutation_version);
        Int64 parts_to_do = versions_it - part_data_versions.begin();
        std::map<String, Int64> block_numbers_map({{"", entry.block_number}});

        for (const MutationCommand & command : entry.commands)
        {
            std::stringstream ss;
            formatAST(*command.ast, ss, false, true);
            result.push_back(MergeTreeMutationStatus
            {
                entry.file_name,
                ss.str(),
                entry.create_time,
                block_numbers_map,
                parts_to_do,
                (parts_to_do == 0),
                entry.latest_failed_part,
                entry.latest_fail_time,
                entry.latest_fail_reason,
            });
        }
    }

    return result;
}

CancellationCode StorageMergeTree::killMutation(const String & mutation_id)
{
    LOG_TRACE(log, "Killing mutation " << mutation_id);

    std::optional<MergeTreeMutationEntry> to_kill;
    {
        std::lock_guard lock(currently_merging_mutex);
        auto it = current_mutations_by_id.find(mutation_id);
        if (it != current_mutations_by_id.end())
        {
            to_kill.emplace(std::move(it->second));
            current_mutations_by_id.erase(it);
            current_mutations_by_version.erase(to_kill->block_number);
        }
    }

    if (!to_kill)
        return CancellationCode::NotFound;

    global_context.getMergeList().cancelPartMutations({}, to_kill->block_number);
    to_kill->removeFile();
    LOG_TRACE(log, "Cancelled part mutations and removed mutation file " << mutation_id);

    /// Maybe there is another mutation that was blocked by the killed one. Try to execute it immediately.
    background_task_handle->wake();

    return CancellationCode::CancelSent;
}


void StorageMergeTree::loadMutations()
{
    Poco::DirectoryIterator end;
    for (auto it = Poco::DirectoryIterator(full_path); it != end; ++it)
    {
        if (startsWith(it.name(), "mutation_"))
        {
            MergeTreeMutationEntry entry(full_path, it.name());
            Int64 block_number = entry.block_number;
            auto insertion = current_mutations_by_id.emplace(it.name(), std::move(entry));
            current_mutations_by_version.emplace(block_number, insertion.first->second);
        }
        else if (startsWith(it.name(), "tmp_mutation_"))
        {
            it->remove();
        }
    }

    if (!current_mutations_by_version.empty())
        increment.value = std::max(Int64(increment.value.load()), current_mutations_by_version.rbegin()->first);
}


bool StorageMergeTree::merge(
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    String * out_disable_reason)
{
    auto table_lock_holder = lockStructureForShare(true, RWLockImpl::NO_QUERY);

    FutureMergedMutatedPart future_part;

    /// You must call destructor with unlocked `currently_merging_mutex`.
    std::optional<CurrentlyMergingPartsTagger> merging_tagger;

    {
        std::lock_guard lock(currently_merging_mutex);

        auto can_merge = [this, &lock] (const DataPartPtr & left, const DataPartPtr & right, String *)
        {
            return !currently_merging.count(left) && !currently_merging.count(right)
                && getCurrentMutationVersion(left, lock) == getCurrentMutationVersion(right, lock);
        };

        bool selected = false;

        if (partition_id.empty())
        {
            UInt64 max_source_parts_size = merger_mutator.getMaxSourcePartsSize();
            if (max_source_parts_size > 0)
                selected = merger_mutator.selectPartsToMerge(future_part, aggressive, max_source_parts_size, can_merge, out_disable_reason);
            else if (out_disable_reason)
                *out_disable_reason = "Current value of max_source_parts_size is zero";
        }
        else
        {
            UInt64 disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);
            selected = merger_mutator.selectAllPartsToMergeWithinPartition(future_part, disk_space, can_merge, partition_id, final, out_disable_reason);
        }

        if (!selected)
            return false;

        merging_tagger.emplace(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part.parts), *this);
    }

    MergeList::EntryPtr merge_entry = global_context.getMergeList().insert(database_name, table_name, future_part);

    /// Logging
    Stopwatch stopwatch;
    MutableDataPartPtr new_part;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        try
        {
            auto part_log = global_context.getPartLog(database_name);
            if (!part_log)
                return;

            PartLogElement part_log_elem;

            part_log_elem.event_type = PartLogElement::MERGE_PARTS;
            part_log_elem.event_time = time(nullptr);
            part_log_elem.duration_ms = stopwatch.elapsed() / 1000000;

            part_log_elem.database_name = database_name;
            part_log_elem.table_name = table_name;
            part_log_elem.partition_id = future_part.part_info.partition_id;
            part_log_elem.part_name = future_part.name;

            if (new_part)
                part_log_elem.bytes_compressed_on_disk = new_part->bytes_on_disk;

            part_log_elem.source_part_names.reserve(future_part.parts.size());
            for (const auto & source_part : future_part.parts)
                part_log_elem.source_part_names.push_back(source_part->name);

            part_log_elem.rows_read = (*merge_entry)->rows_read;
            part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

            part_log_elem.rows = (*merge_entry)->rows_written;
            part_log_elem.bytes_uncompressed = (*merge_entry)->bytes_written_uncompressed;

            part_log_elem.error = static_cast<UInt16>(execution_status.code);
            part_log_elem.exception = execution_status.message;

            part_log->add(part_log_elem);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    };

    try
    {
        new_part = merger_mutator.mergePartsToTemporaryPart(
            future_part, *merge_entry, time(nullptr),
            merging_tagger->reserved_space.get(), deduplicate);
        merger_mutator.renameMergedTemporaryPart(new_part, future_part.parts, nullptr);
        removeEmptyColumnsFromPart(new_part);

        merging_tagger->is_successful = true;
        write_part_log({});
    }
    catch (...)
    {
        merging_tagger->exception_message = getCurrentExceptionMessage(false);
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}


bool StorageMergeTree::tryMutatePart()
{
    auto table_lock_holder = lockStructureForShare(true, RWLockImpl::NO_QUERY);

    FutureMergedMutatedPart future_part;
    MutationCommands commands;
    /// You must call destructor with unlocked `currently_merging_mutex`.
    std::optional<CurrentlyMergingPartsTagger> tagger;
    {
        auto disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

        std::lock_guard lock(currently_merging_mutex);

        if (current_mutations_by_version.empty())
            return false;

        auto mutations_end_it = current_mutations_by_version.end();
        for (const auto & part : getDataPartsVector())
        {
            if (currently_merging.count(part))
                continue;

            auto mutations_begin_it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
            if (mutations_begin_it == mutations_end_it)
                continue;

            auto estimated_needed_space = MergeTreeDataMergerMutator::estimateNeededDiskSpace({part});
            if (estimated_needed_space > disk_space)
                continue;

            for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
                commands.insert(commands.end(), it->second.commands.begin(), it->second.commands.end());

            auto new_part_info = part->info;
            new_part_info.mutation = current_mutations_by_version.rbegin()->first;

            future_part.parts.push_back(part);
            future_part.part_info = new_part_info;
            future_part.name = part->getNewName(new_part_info);

            tagger.emplace(future_part, estimated_needed_space, *this);
            break;
        }
    }

    if (!tagger)
        return false;

    MergeList::EntryPtr merge_entry = global_context.getMergeList().insert(database_name, table_name, future_part);

    Stopwatch stopwatch;
    MutableDataPartPtr new_part;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        try
        {
            auto part_log = global_context.getPartLog(database_name);
            if (!part_log)
                return;

            PartLogElement part_log_elem;

            part_log_elem.event_type = PartLogElement::MUTATE_PART;

            part_log_elem.error = static_cast<UInt16>(execution_status.code);
            part_log_elem.exception = execution_status.message;

            part_log_elem.event_time = time(nullptr);
            part_log_elem.duration_ms = stopwatch.elapsed() / 1000000;

            part_log_elem.database_name = database_name;
            part_log_elem.table_name = table_name;
            part_log_elem.partition_id = future_part.part_info.partition_id;
            part_log_elem.part_name = future_part.name;

            part_log_elem.rows_read = (*merge_entry)->rows_read;
            part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

            part_log_elem.rows = (*merge_entry)->rows_written;
            part_log_elem.bytes_uncompressed = (*merge_entry)->bytes_written_uncompressed;

            if (new_part)
                part_log_elem.bytes_compressed_on_disk = new_part->bytes_on_disk;

            part_log_elem.source_part_names.reserve(future_part.parts.size());
            for (const auto & source_part : future_part.parts)
                part_log_elem.source_part_names.push_back(source_part->name);

            part_log->add(part_log_elem);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    };

    try
    {
        new_part = merger_mutator.mutatePartToTemporaryPart(future_part, commands, *merge_entry, global_context);
        renameTempPartAndReplace(new_part);
        tagger->is_successful = true;
        write_part_log({});
    }
    catch (...)
    {
        tagger->exception_message = getCurrentExceptionMessage(false);
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}


BackgroundProcessingPoolTaskResult StorageMergeTree::backgroundTask()
{
    if (shutdown_called)
        return BackgroundProcessingPoolTaskResult::ERROR;

    if (merger_mutator.actions_blocker.isCancelled())
        return BackgroundProcessingPoolTaskResult::ERROR;

    try
    {
        /// Clear old parts. It is unnecessary to do it more than once a second.
        if (auto lock = time_after_previous_cleanup.compareAndRestartDeferred(1))
        {
            clearOldPartsFromFilesystem();
            {
                /// TODO: Implement tryLockStructureForShare.
                auto lock_structure = lockStructureForShare(false, "");
                clearOldTemporaryDirectories();
            }
            clearOldMutations();
        }

        ///TODO: read deduplicate option from table config
        if (merge(false /*aggressive*/, {} /*partition_id*/, false /*final*/, false /*deduplicate*/))
            return BackgroundProcessingPoolTaskResult::SUCCESS;

        if (tryMutatePart())
            return BackgroundProcessingPoolTaskResult::SUCCESS;
        else
            return BackgroundProcessingPoolTaskResult::ERROR;
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ABORTED)
        {
            LOG_INFO(log, e.message());
            return BackgroundProcessingPoolTaskResult::ERROR;
        }

        throw;
    }
}

Int64 StorageMergeTree::getCurrentMutationVersion(
    const DataPartPtr & part,
    std::lock_guard<std::mutex> & /* currently_merging_mutex_lock */) const
{
    auto it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
    if (it == current_mutations_by_version.begin())
        return 0;
    --it;
    return it->first;
}

void StorageMergeTree::clearOldMutations()
{
    if (!settings.finished_mutations_to_keep)
        return;

    std::vector<MergeTreeMutationEntry> mutations_to_delete;
    {
        std::lock_guard lock(currently_merging_mutex);

        if (current_mutations_by_version.size() <= settings.finished_mutations_to_keep)
            return;

        auto begin_it = current_mutations_by_version.begin();

        std::optional<Int64> min_version = getMinPartDataVersion();
        auto end_it = current_mutations_by_version.end();
        if (min_version)
            end_it = current_mutations_by_version.upper_bound(*min_version);

        size_t done_count = std::distance(begin_it, end_it);
        if (done_count <= settings.finished_mutations_to_keep)
            return;

        size_t to_delete_count = done_count - settings.finished_mutations_to_keep;

        auto it = begin_it;
        for (size_t i = 0; i < to_delete_count; ++i)
        {
            mutations_to_delete.push_back(std::move(it->second));
            current_mutations_by_id.erase(mutations_to_delete.back().file_name);
            it = current_mutations_by_version.erase(it);
        }
    }

    for (auto & mutation : mutations_to_delete)
    {
        LOG_TRACE(log, "Removing mutation: " << mutation.file_name);
        mutation.removeFile();
    }
}


void StorageMergeTree::clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context)
{
    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger_mutator.actions_blocker.cancel();

    /// We don't change table structure, only data in some parts, parts are locked inside alterDataPart() function
    auto lock_read_structure = lockStructureForShare(false, context.getCurrentQueryId());

    String partition_id = getPartitionIDFromQuery(partition, context);
    auto parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    std::vector<AlterDataPartTransactionPtr> transactions;

    AlterCommand alter_command;
    alter_command.type = AlterCommand::DROP_COLUMN;
    alter_command.column_name = get<String>(column_name);

    auto new_columns = getColumns();
    auto new_indices = getIndices();
    ASTPtr ignored_order_by_ast;
    ASTPtr ignored_primary_key_ast;
    ASTPtr ignored_ttl_table_ast;
    alter_command.apply(new_columns, new_indices, ignored_order_by_ast, ignored_primary_key_ast, ignored_ttl_table_ast);

    auto columns_for_parts = new_columns.getAllPhysical();
    for (const auto & part : parts)
    {
        if (part->info.partition_id != partition_id)
            throw Exception("Unexpected partition ID " + part->info.partition_id + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);

        MergeTreeData::AlterDataPartTransactionPtr transaction(new MergeTreeData::AlterDataPartTransaction(part));
        alterDataPart(columns_for_parts, new_indices.indices, false, transaction);
        if (transaction->isValid())
            transactions.push_back(std::move(transaction));

        LOG_DEBUG(log, "Removing column " << get<String>(column_name) << " from part " << part->name);
    }

    if (transactions.empty())
        return;

    for (auto & transaction : transactions)
    {
        transaction->commit();
        transaction.reset();
    }

    /// Recalculate columns size (not only for the modified column)
    recalculateColumnSizes();
}


bool StorageMergeTree::optimize(
    const ASTPtr & /*query*/, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    String disable_reason;
    if (!partition && final)
    {
        DataPartsVector data_parts = getDataPartsVector();
        std::unordered_set<String> partition_ids;

        for (const DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);

        for (const String & partition_id : partition_ids)
        {
            if (!merge(true, partition_id, true, deduplicate, &disable_reason))
            {
                if (context.getSettingsRef().optimize_throw_if_noop)
                    throw Exception(disable_reason.empty() ? "Can't OPTIMIZE by some reason" : disable_reason, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
                return false;
            }
        }
    }
    else
    {
        String partition_id;
        if (partition)
            partition_id = getPartitionIDFromQuery(partition, context);

        if (!merge(true, partition_id, final, deduplicate, &disable_reason))
        {
            if (context.getSettingsRef().optimize_throw_if_noop)
                throw Exception(disable_reason.empty() ? "Can't OPTIMIZE by some reason" : disable_reason, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
            return false;
        }
    }

    return true;
}

void StorageMergeTree::alterPartition(const ASTPtr & query, const PartitionCommands & commands, const Context & context)
{
    for (const PartitionCommand & command : commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
                checkPartitionCanBeDropped(command.partition);
                dropPartition(command.partition, command.detach, context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                attachPartition(command.partition, command.part, context);
                break;

            case PartitionCommand::REPLACE_PARTITION:
            {
                checkPartitionCanBeDropped(command.partition);
                String from_database = command.from_database.empty() ? context.getCurrentDatabase() : command.from_database;
                auto from_storage = context.getTable(from_database, command.from_table);
                replacePartitionFrom(from_storage, command.partition, command.replace, context);
            }
            break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockStructureForShare(false, context.getCurrentQueryId());
                freezePartition(command.partition, command.with_name, context);
            }
            break;

            case PartitionCommand::CLEAR_COLUMN:
                clearColumnInPartition(command.partition, command.column_name, context);
                break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockStructureForShare(false, context.getCurrentQueryId());
                freezeAll(command.with_name, context);
            }
            break;

            default:
                IStorage::alterPartition(query, commands, context); // should throw an exception.
        }
    }
}

void StorageMergeTree::dropPartition(const ASTPtr & partition, bool detach, const Context & context)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.actions_blocker.cancel();
        /// Waits for completion of merge and does not start new ones.
        auto lock = lockExclusively(context.getCurrentQueryId());

        String partition_id = getPartitionIDFromQuery(partition, context);

        /// TODO: should we include PreComitted parts like in Replicated case?
        auto parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
        removePartsFromWorkingSet(parts_to_remove, true);

        if (detach)
        {
            /// If DETACH clone parts to detached/ directory
            for (const auto & part : parts_to_remove)
            {
                LOG_INFO(log, "Detaching " << part->relative_path);
                part->makeCloneInDetached("");
            }
        }

        LOG_INFO(log, (detach ? "Detached " : "Removed ") << parts_to_remove.size() << " parts inside partition ID " << partition_id << ".");
    }

    clearOldPartsFromFilesystem();
}


void StorageMergeTree::attachPartition(const ASTPtr & partition, bool attach_part, const Context & context)
{
    // TODO: should get some locks to prevent race with 'alter … modify column'

    String partition_id;

    if (attach_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, context);

    String source_dir = "detached/";

    /// Let's make a list of parts to add.
    Strings parts;
    if (attach_part)
    {
        parts.push_back(partition_id);
    }
    else
    {
        LOG_DEBUG(log, "Looking for parts for partition " << partition_id << " in " << source_dir);
        ActiveDataPartSet active_parts(format_version);
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            const String & name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info, format_version)
                || part_info.partition_id != partition_id)
            {
                continue;
            }
            LOG_DEBUG(log, "Found part " << name);
            active_parts.add(name);
        }
        LOG_DEBUG(log, active_parts.size() << " of them are active");
        parts = active_parts.getParts();
    }

    for (const auto & source_part_name : parts)
    {
        String source_path = source_dir + source_part_name;

        LOG_DEBUG(log, "Checking data");
        MutableDataPartPtr part = loadPartAndFixMetadata(source_path);

        LOG_INFO(log, "Attaching part " << source_part_name << " from " << source_path);
        renameTempPartAndAdd(part, &increment);

        LOG_INFO(log, "Finished attaching part");
    }

    /// New parts with other data may appear in place of deleted parts.
    context.dropCaches();
}

void StorageMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context)
{
    auto lock1 = lockStructureForShare(false, context.getCurrentQueryId());
    auto lock2 = source_table->lockStructureForShare(false, context.getCurrentQueryId());

    Stopwatch watch;
    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table);
    String partition_id = getPartitionIDFromQuery(partition, context);

    DataPartsVector src_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    MutableDataPartsVector dst_parts;

    static const String TMP_PREFIX = "tmp_replace_from_";

    for (const DataPartPtr & src_part : src_parts)
    {
        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        std::shared_lock<std::shared_mutex> part_lock(src_part->columns_lock);
        dst_parts.emplace_back(cloneAndLoadDataPart(src_part, TMP_PREFIX, dst_part_info));
    }

    /// ATTACH empty part set
    if (!replace && dst_parts.empty())
        return;

    MergeTreePartInfo drop_range;
    if (replace)
    {
        drop_range.partition_id = partition_id;
        drop_range.min_block = 0;
        drop_range.max_block = increment.get(); // there will be a "hole" in block numbers
        drop_range.level = std::numeric_limits<decltype(drop_range.level)>::max();
    }

    /// Atomically add new parts and remove old ones
    try
    {
        {
            /// Here we use the transaction just like RAII since rare errors in renameTempPartAndReplace() are possible
            ///  and we should be able to rollback already added (Precomitted) parts
            Transaction transaction(*this);

            auto data_parts_lock = lockParts();

            /// Populate transaction
            for (MutableDataPartPtr & part : dst_parts)
                renameTempPartAndReplace(part, &increment, &transaction, data_parts_lock);

            transaction.commit(&data_parts_lock);

            /// If it is REPLACE (not ATTACH), remove all parts which max_block_number less then min_block_number of the first new block
            if (replace)
                removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(global_context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(global_context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }
}

ActionLock StorageMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.actions_blocker.cancel();

    return {};
}

}
