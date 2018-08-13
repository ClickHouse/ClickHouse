#include <optional>
#include <Common/FieldVisitors.h>
#include <Common/localBackup.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeList.h>
#include <Databases/IDatabase.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Parsers/queryToString.h>


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
    bool attach,
    Context & context_,
    const ASTPtr & primary_expr_ast_,
    const ASTPtr & secondary_sorting_expr_list_,
    const String & date_column_name,
    const ASTPtr & partition_expr_ast_,
    const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
    const MergeTreeData::MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag)
    : path(path_), database_name(database_name_), table_name(table_name_), full_path(path + escapeForFileName(table_name) + '/'),
    context(context_), background_pool(context_.getBackgroundPool()),
    data(database_name, table_name,
         full_path, columns_,
         context_, primary_expr_ast_, secondary_sorting_expr_list_, date_column_name, partition_expr_ast_,
         sampling_expression_, merging_params_,
         settings_, false, attach),
    reader(data), writer(data), merger_mutator(data, context.getBackgroundPool()),
    log(&Logger::get(database_name_ + "." + table_name + " (StorageMergeTree)"))
{
    if (path_.empty())
        throw Exception("MergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    data.loadDataParts(has_force_restore_data_flag);

    if (!attach && !data.getDataParts().empty())
        throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

    increment.set(data.getMaxBlockNumber());

    loadMutations();
}


void StorageMergeTree::startup()
{
    background_task_handle = background_pool.addTask([this] { return backgroundTask(); });

    data.clearOldPartsFromFilesystem();

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    data.clearOldTemporaryDirectories(0);
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
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    return reader.read(column_names, query_info, context, processed_stage, max_block_size, num_streams, 0);
}

BlockOutputStreamPtr StorageMergeTree::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    return std::make_shared<MergeTreeBlockOutputStream>(*this);
}

void StorageMergeTree::checkTableCanBeDropped() const
{
    const_cast<MergeTreeData &>(getData()).recalculateColumnSizes();
    context.checkTableCanBeDropped(database_name, table_name, getData().getTotalActiveSizeInBytes());
}

void StorageMergeTree::checkPartitionCanBeDropped(const ASTPtr & partition)
{
    const_cast<MergeTreeData &>(getData()).recalculateColumnSizes();

    const String partition_id = data.getPartitionIDFromQuery(partition, context);
    auto parts_to_remove = data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
    {
        partition_size += part->bytes_on_disk;
    }
    context.checkPartitionCanBeDropped(database_name, table_name, partition_size);
}

void StorageMergeTree::drop()
{
    shutdown();
    data.dropAllData();
}

void StorageMergeTree::truncate(const ASTPtr &)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.actions_blocker.cancel();

        /// NOTE: It's assumed that this method is called under lockForAlter.

        auto parts_to_remove = data.getDataPartsVector();
        data.removePartsFromWorkingSet(parts_to_remove, true);

        LOG_INFO(log, "Removed " << parts_to_remove.size() << " parts.");
    }

    data.clearOldPartsFromFilesystem();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

    data.setPath(new_full_path);

    path = new_path_to_db;
    table_name = new_table_name;
    full_path = new_full_path;

    /// NOTE: Logger names are not updated.
}


void StorageMergeTree::alter(
    const AlterCommands & params,
    const String & database_name,
    const String & table_name,
    const Context & context)
{
    /// NOTE: Here, as in ReplicatedMergeTree, you can do ALTER which does not block the writing of data for a long time.
    auto merge_blocker = merger_mutator.actions_blocker.cancel();

    auto table_soft_lock = lockDataForAlter(__PRETTY_FUNCTION__);

    data.checkAlter(params);

    auto new_columns = data.getColumns();
    params.apply(new_columns);

    std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;

    bool primary_key_is_modified = false;

    ASTPtr new_primary_key_ast = data.primary_expr_ast;

    for (const AlterCommand & param : params)
    {
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
        {
            primary_key_is_modified = true;
            new_primary_key_ast = param.primary_key;
        }
    }

    if (primary_key_is_modified && supportsSampling())
        throw Exception("MODIFY PRIMARY KEY only supported for tables without sampling key", ErrorCodes::BAD_ARGUMENTS);

    auto parts = data.getDataParts({MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});
    auto columns_for_parts = new_columns.getAllPhysical();
    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        if (auto transaction = data.alterDataPart(part, columns_for_parts, new_primary_key_ast, false))
            transactions.push_back(std::move(transaction));
    }

    auto table_hard_lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    IDatabase::ASTModifier storage_modifier;
    if (primary_key_is_modified)
    {
        storage_modifier = [&new_primary_key_ast] (IAST & ast)
        {
            auto tuple = std::make_shared<ASTFunction>();
            tuple->name = "tuple";
            tuple->arguments = new_primary_key_ast;
            tuple->children.push_back(tuple->arguments);

            /// Primary key is in the second place in table engine description and can be represented as a tuple.
            /// TODO: Not always in second place. If there is a sampling key, then the third one. Fix it.
            auto & storage_ast = typeid_cast<ASTStorage &>(ast);
            typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments).children.at(1) = tuple;
        };
    }

    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, storage_modifier);
    setColumns(std::move(new_columns));

    if (primary_key_is_modified)
    {
        data.primary_expr_ast = new_primary_key_ast;
    }
    /// Reinitialize primary key because primary key column types might have changed.
    data.initPrimaryKey();

    for (auto & transaction : transactions)
        transaction->commit();

    /// Columns sizes could be changed
    data.recalculateColumnSizes();

    if (primary_key_is_modified)
        data.loadDataParts(false);
}


/// While exists, marks parts as 'currently_merging' and reserves free space on filesystem.
/// It's possible to mark parts before.
struct CurrentlyMergingPartsTagger
{
    MergeTreeData::DataPartsVector parts;
    DiskSpaceMonitor::ReservationPtr reserved_space;
    StorageMergeTree * storage = nullptr;

    CurrentlyMergingPartsTagger() = default;

    CurrentlyMergingPartsTagger(const MergeTreeData::DataPartsVector & parts_, size_t total_size, StorageMergeTree & storage_)
        : parts(parts_), storage(&storage_)
    {
        /// Assume mutex is already locked, because this method is called from mergeTask.
        reserved_space = DiskSpaceMonitor::reserve(storage->full_path, total_size); /// May throw.
        for (const auto & part : parts)
        {
            if (storage->currently_merging.count(part))
                throw Exception("Tagging alreagy tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
        }
        storage->currently_merging.insert(parts.begin(), parts.end());
    }

    ~CurrentlyMergingPartsTagger()
    {
        std::lock_guard<std::mutex> lock(storage->currently_merging_mutex);

        for (const auto & part : parts)
        {
            if (!storage->currently_merging.count(part))
                std::terminate();
            storage->currently_merging.erase(part);
        }
    }
};


void StorageMergeTree::mutate(const MutationCommands & commands, const Context &)
{
    MergeTreeMutationEntry entry(commands, full_path, data.insert_increment.get());
    {
        std::lock_guard lock(currently_merging_mutex);

        Int64 version = increment.get();
        entry.commit(version);
        current_mutations_by_version.emplace(version, std::move(entry));
    }

    LOG_INFO(log, "Added mutation: " << entry.file_name);
    background_task_handle->wake();
}


std::vector<MergeTreeMutationStatus> StorageMergeTree::getMutationsStatus() const
{
    std::lock_guard lock(currently_merging_mutex);

    std::vector<Int64> part_data_versions;
    auto data_parts = data.getDataPartsVector();
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
            });
        }
    }

    return result;
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
            current_mutations_by_version.emplace(block_number, std::move(entry));
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
    size_t aio_threshold,
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    String * out_disable_reason)
{
    auto structure_lock = lockStructure(true, __PRETTY_FUNCTION__);

    MergeTreeDataMergerMutator::FuturePart future_part;

    /// You must call destructor with unlocked `currently_merging_mutex`.
    std::optional<CurrentlyMergingPartsTagger> merging_tagger;

    {
        std::lock_guard<std::mutex> lock(currently_merging_mutex);

        auto can_merge = [this, &lock] (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right, String *)
        {
            return !currently_merging.count(left) && !currently_merging.count(right)
                && getCurrentMutationVersion(left, lock) == getCurrentMutationVersion(right, lock);
        };

        bool selected = false;

        if (partition_id.empty())
        {
            size_t max_source_parts_size = merger_mutator.getMaxSourcePartsSize();
            if (max_source_parts_size > 0)
                selected = merger_mutator.selectPartsToMerge(future_part, aggressive, max_source_parts_size, can_merge, out_disable_reason);
        }
        else
        {
            size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);
            selected = merger_mutator.selectAllPartsToMergeWithinPartition(future_part, disk_space, can_merge, partition_id, final, out_disable_reason);
        }

        if (!selected)
            return false;

        merging_tagger.emplace(future_part.parts, MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part.parts), *this);
    }

    MergeList::EntryPtr merge_entry = context.getMergeList().insert(database_name, table_name, future_part.name, future_part.parts);

    /// Logging
    Stopwatch stopwatch;
    MergeTreeData::MutableDataPartPtr new_part;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        try
        {
            auto part_log = context.getPartLog(database_name);
            if (!part_log)
                return;

            PartLogElement part_log_elem;

            part_log_elem.event_type = PartLogElement::MERGE_PARTS;
            part_log_elem.event_time = time(nullptr);
            part_log_elem.duration_ms = stopwatch.elapsed() / 1000000;

            part_log_elem.database_name = database_name;
            part_log_elem.table_name = table_name;
            part_log_elem.part_name = future_part.name;

            if (new_part)
                part_log_elem.bytes_compressed_on_disk = new_part->bytes_on_disk;

            part_log_elem.source_part_names.reserve(future_part.parts.size());
            for (const auto & source_part : future_part.parts)
                part_log_elem.source_part_names.push_back(source_part->name);

            part_log_elem.rows_read = (*merge_entry)->bytes_read_uncompressed;
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
            future_part, *merge_entry, aio_threshold, time(nullptr),
            merging_tagger->reserved_space.get(), deduplicate);
        merger_mutator.renameMergedTemporaryPart(new_part, future_part.parts, nullptr);

        write_part_log({});
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}


bool StorageMergeTree::tryMutatePart()
{
    auto structure_lock = lockStructure(true, __PRETTY_FUNCTION__);

    MergeTreeDataMergerMutator::FuturePart future_part;
    MutationCommands commands;
    /// You must call destructor with unlocked `currently_merging_mutex`.
    std::optional<CurrentlyMergingPartsTagger> tagger;
    {
        auto disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

        std::lock_guard<std::mutex> lock(currently_merging_mutex);

        if (current_mutations_by_version.empty())
            return false;

        auto mutations_end_it = current_mutations_by_version.end();
        for (const auto & part : data.getDataPartsVector())
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

            tagger.emplace({part}, estimated_needed_space, *this);
            break;
        }
    }

    if (!tagger)
        return false;

    Stopwatch stopwatch;
    MergeTreeData::MutableDataPartPtr new_part;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        try
        {
            auto part_log = context.getPartLog(database_name);
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
            part_log_elem.part_name = future_part.name;

            if (new_part)
            {
                part_log_elem.bytes_compressed_on_disk = new_part->bytes_on_disk;
                part_log_elem.rows = new_part->rows_count;
            }

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
        new_part = merger_mutator.mutatePartToTemporaryPart(future_part, commands, context);
        data.renameTempPartAndReplace(new_part);
        write_part_log({});
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}


bool StorageMergeTree::backgroundTask()
{
    if (shutdown_called)
        return false;

    if (merger_mutator.actions_blocker.isCancelled())
        return false;

    try
    {
        /// Clear old parts. It is unnecessary to do it more than once a second.
        if (auto lock = time_after_previous_cleanup.compareAndRestartDeferred(1))
        {
            data.clearOldPartsFromFilesystem();
            data.clearOldTemporaryDirectories();
            clearOldMutations();
        }

        size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
        ///TODO: read deduplicate option from table config
        if (merge(aio_threshold, false /*aggressive*/, {} /*partition_id*/, false /*final*/, false /*deduplicate*/))
            return true;

        return tryMutatePart();
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ABORTED)
        {
            LOG_INFO(log, e.message());
            return false;
        }

        throw;
    }
}

Int64 StorageMergeTree::getCurrentMutationVersion(
    const MergeTreeData::DataPartPtr & part,
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
    if (!data.settings.finished_mutations_to_keep)
        return;

    std::vector<MergeTreeMutationEntry> mutations_to_delete;
    {
        std::lock_guard lock(currently_merging_mutex);

        if (current_mutations_by_version.size() <= data.settings.finished_mutations_to_keep)
            return;

        auto begin_it = current_mutations_by_version.begin();

        std::optional<Int64> min_version = data.getMinPartDataVersion();
        auto end_it = current_mutations_by_version.end();
        if (min_version)
            end_it = current_mutations_by_version.upper_bound(*min_version);

        size_t done_count = std::distance(begin_it, end_it);
        if (done_count <= data.settings.finished_mutations_to_keep)
            return;

        size_t to_delete_count = done_count - data.settings.finished_mutations_to_keep;

        auto it = begin_it;
        for (size_t i = 0; i < to_delete_count; ++i)
        {
            mutations_to_delete.push_back(std::move(it->second));
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
    auto lock_read_structure = lockStructure(false, __PRETTY_FUNCTION__);

    String partition_id = data.getPartitionIDFromQuery(partition, context);
    auto parts = data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);

    std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;

    AlterCommand alter_command;
    alter_command.type = AlterCommand::DROP_COLUMN;
    alter_command.column_name = get<String>(column_name);

    auto new_columns = getColumns();
    alter_command.apply(new_columns);

    auto columns_for_parts = new_columns.getAllPhysical();
    for (const auto & part : parts)
    {
        if (part->info.partition_id != partition_id)
            throw Exception("Unexpected partition ID " + part->info.partition_id + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);

        if (auto transaction = data.alterDataPart(part, columns_for_parts, data.primary_expr_ast, false))
            transactions.push_back(std::move(transaction));

        LOG_DEBUG(log, "Removing column " << get<String>(column_name) << " from part " << part->name);
    }

    if (transactions.empty())
        return;

    for (auto & transaction : transactions)
        transaction->commit();

    /// Recalculate columns size (not only for the modified column)
    data.recalculateColumnSizes();
}


bool StorageMergeTree::optimize(
    const ASTPtr & /*query*/, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    String partition_id;
    if (partition)
        partition_id = data.getPartitionIDFromQuery(partition, context);

    String disable_reason;
    if (!partition && final)
    {
        MergeTreeData::DataPartsVector data_parts = data.getDataPartsVector();
        std::unordered_set<String> partition_ids;

        for (const MergeTreeData::DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);

        for (const String & partition_id : partition_ids)
        {
            if (!merge(context.getSettingsRef().min_bytes_to_use_direct_io, true, partition_id, true, deduplicate, &disable_reason))
            {
                if (context.getSettingsRef().optimize_throw_if_noop)
                    throw Exception(disable_reason.empty() ? "Can't OPTIMIZE by some reason" : disable_reason, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
                return false;
            }
        }
    }
    else
    {
        if (!merge(context.getSettingsRef().min_bytes_to_use_direct_io, true, partition_id, final, deduplicate, &disable_reason))
        {
            if (context.getSettingsRef().optimize_throw_if_noop)
                throw Exception(disable_reason.empty() ? "Can't OPTIMIZE by some reason" : disable_reason, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
            return false;
        }
    }

    return true;
}


void StorageMergeTree::dropPartition(const ASTPtr & /*query*/, const ASTPtr & partition, bool detach, const Context & context)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = merger_mutator.actions_blocker.cancel();
        /// Waits for completion of merge and does not start new ones.
        auto lock = lockForAlter(__PRETTY_FUNCTION__);

        String partition_id = data.getPartitionIDFromQuery(partition, context);

        /// TODO: should we include PreComitted parts like in Replicated case?
        auto parts_to_remove = data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
        data.removePartsFromWorkingSet(parts_to_remove, true);

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

    data.clearOldPartsFromFilesystem();
}


void StorageMergeTree::attachPartition(const ASTPtr & partition, bool part, const Context & context)
{
    String partition_id;

    if (part)
        partition_id = typeid_cast<const ASTLiteral &>(*partition).value.safeGet<String>();
    else
        partition_id = data.getPartitionIDFromQuery(partition, context);

    String source_dir = "detached/";

    /// Let's make a list of parts to add.
    Strings parts;
    if (part)
    {
        parts.push_back(partition_id);
    }
    else
    {
        LOG_DEBUG(log, "Looking for parts for partition " << partition_id << " in " << source_dir);
        ActiveDataPartSet active_parts(data.format_version);
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            const String & name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info, data.format_version)
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
        MergeTreeData::MutableDataPartPtr part = data.loadPartAndFixMetadata(source_path);

        LOG_INFO(log, "Attaching part " << source_part_name << " from " << source_path);
        data.renameTempPartAndAdd(part, &increment);

        LOG_INFO(log, "Finished attaching part");
    }

    /// New parts with other data may appear in place of deleted parts.
    context.dropCaches();
}

void StorageMergeTree::freezePartition(const ASTPtr & partition, const String & with_name, const Context & context)
{
    data.freezePartition(partition, with_name, context);
}

void StorageMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context)
{
    auto lock1 = lockStructure(false, __PRETTY_FUNCTION__);
    auto lock2 = source_table->lockStructure(false, __PRETTY_FUNCTION__);

    Stopwatch watch;
    MergeTreeData * src_data = data.checkStructureAndGetMergeTreeData(source_table);
    String partition_id = data.getPartitionIDFromQuery(partition, context);

    MergeTreeData::DataPartsVector src_parts = src_data->getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    MergeTreeData::MutableDataPartsVector dst_parts;

    static const String TMP_PREFIX = "tmp_replace_from_";

    for (const MergeTreeData::DataPartPtr & src_part : src_parts)
    {
        /// This will generate unique name in scope of current server process.
        Int64 temp_index = data.insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        std::shared_lock<std::shared_mutex> part_lock(src_part->columns_lock);
        dst_parts.emplace_back(data.cloneAndLoadDataPart(src_part, TMP_PREFIX, dst_part_info));
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
            MergeTreeData::Transaction transaction;

            auto data_parts_lock = data.lockParts();

            /// Populate transaction
            for (MergeTreeData::MutableDataPartPtr & part : dst_parts)
                data.renameTempPartAndReplace(part, &increment, &transaction, data_parts_lock);

            transaction.commit(&data_parts_lock);

            /// If it is REPLACE (not ATTACH), remove all parts which max_block_number less then min_block_number of the first new block
            if (replace)
                data.removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(this->context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(this->context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
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
