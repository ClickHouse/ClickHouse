#include <experimental/optional>
#include <Core/FieldVisitors.h>
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
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}


StorageMergeTree::StorageMergeTree(
    const String & path_,
    const String & database_name_,
    const String & table_name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach,
    Context & context_,
    ASTPtr & primary_expr_ast_,
    const String & date_column_name_,
    const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
    size_t index_granularity_,
    const MergeTreeData::MergingParams & merging_params_,
    bool has_force_restore_data_flag,
    const MergeTreeSettings & settings_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    path(path_), database_name(database_name_), table_name(table_name_), full_path(path + escapeForFileName(table_name) + '/'),
    context(context_), background_pool(context_.getBackgroundPool()),
    data(database_name, table_name,
         full_path, columns_,
         materialized_columns_, alias_columns_, column_defaults_,
         context_, primary_expr_ast_, date_column_name_,
         sampling_expression_, index_granularity_, merging_params_,
         settings_, database_name_ + "." + table_name, false, attach),
    reader(data), writer(data), merger(data, context.getBackgroundPool()),
    log(&Logger::get(database_name_ + "." + table_name + " (StorageMergeTree)"))
{
    data.loadDataParts(has_force_restore_data_flag);

    if (!attach)
    {
        if (!data.getDataParts().empty())
            throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);
    }
    else
    {
        data.clearOldParts();
    }

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    data.clearOldTemporaryDirectories(0);

    increment.set(data.getMaxDataPartIndex());
}


void StorageMergeTree::startup()
{
    merge_task_handle = background_pool.addTask([this] { return mergeTask(); });
}


void StorageMergeTree::shutdown()
{
    if (shutdown_called)
        return;
    shutdown_called = true;
    merger.cancelForever();
    if (merge_task_handle)
        background_pool.removeTask(merge_task_handle);
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
    return reader.read(column_names, query_info, context, processed_stage, max_block_size, num_streams, nullptr, 0);
}

BlockOutputStreamPtr StorageMergeTree::write(const ASTPtr & query, const Settings & settings)
{
    return std::make_shared<MergeTreeBlockOutputStream>(*this);
}

bool StorageMergeTree::checkTableCanBeDropped() const
{
    const_cast<MergeTreeData &>(getData()).recalculateColumnSizes();
    context.checkTableCanBeDropped(database_name, table_name, getData().getTotalCompressedSize());
    return true;
}

void StorageMergeTree::drop()
{
    shutdown();
    data.dropAllData();
}

void StorageMergeTree::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
    std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

    data.setPath(new_full_path, true);

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
    auto merge_blocker = merger.cancel();

    auto table_soft_lock = lockDataForAlter();

    data.checkAlter(params);

    auto new_columns = data.getColumnsListNonMaterialized();
    auto new_materialized_columns = data.materialized_columns;
    auto new_alias_columns = data.alias_columns;
    auto new_column_defaults = data.column_defaults;

    params.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

    auto columns_for_parts = new_columns;
    columns_for_parts.insert(std::end(columns_for_parts),
        std::begin(new_materialized_columns), std::end(new_materialized_columns));

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

    if (primary_key_is_modified && data.merging_params.mode == MergeTreeData::MergingParams::Unsorted)
        throw Exception("UnsortedMergeTree cannot have primary key", ErrorCodes::BAD_ARGUMENTS);

    if (primary_key_is_modified && supportsSampling())
        throw Exception("MODIFY PRIMARY KEY only supported for tables without sampling key", ErrorCodes::BAD_ARGUMENTS);

    MergeTreeData::DataParts parts = data.getAllDataParts();
    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        if (auto transaction = data.alterDataPart(part, columns_for_parts, new_primary_key_ast, false))
            transactions.push_back(std::move(transaction));
    }

    auto table_hard_lock = lockStructureForAlter();

    IDatabase::ASTModifier engine_modifier;
    if (primary_key_is_modified)
        engine_modifier = [&new_primary_key_ast] (ASTPtr & engine_ast)
        {
            auto tuple = std::make_shared<ASTFunction>(new_primary_key_ast->range);
            tuple->name = "tuple";
            tuple->arguments = new_primary_key_ast;
            tuple->children.push_back(tuple->arguments);

            /// Primary key is in the second place in table engine description and can be represented as a tuple.
            /// TODO: Not always in second place. If there is a sampling key, then the third one. Fix it.
            typeid_cast<ASTExpressionList &>(*typeid_cast<ASTFunction &>(*engine_ast).arguments).children.at(1) = tuple;
        };

    context.getDatabase(database_name)->alterTable(
        context, table_name,
        new_columns, new_materialized_columns, new_alias_columns, new_column_defaults,
        engine_modifier);

    materialized_columns = new_materialized_columns;
    alias_columns = new_alias_columns;
    column_defaults = new_column_defaults;

    data.setColumnsList(new_columns);
    data.materialized_columns = std::move(new_materialized_columns);
    data.alias_columns = std::move(new_alias_columns);
    data.column_defaults = std::move(new_column_defaults);

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


bool StorageMergeTree::merge(
    size_t aio_threshold,
    bool aggressive,
    const String & partition,
    bool final,
    bool deduplicate)
{
    /// Clear old parts. It does not matter to do it more frequently than each second.
    if (auto lock = time_after_previous_cleanup.lockTestAndRestartAfter(1))
    {
        data.clearOldParts();
        data.clearOldTemporaryDirectories();
    }

    auto structure_lock = lockStructure(true);

    size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

    /// You must call destructor under unlocked `currently_merging_mutex`.
    std::experimental::optional<CurrentlyMergingPartsTagger> merging_tagger;
    String merged_name;

    {
        std::lock_guard<std::mutex> lock(currently_merging_mutex);

        MergeTreeData::DataPartsVector parts;

        auto can_merge = [this] (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
        {
            return !currently_merging.count(left) && !currently_merging.count(right);
        };

        bool selected = false;

        if (partition.empty())
        {
            size_t max_parts_size_for_merge = merger.getMaxPartsSizeForMerge();
            if (max_parts_size_for_merge > 0)
                selected = merger.selectPartsToMerge(parts, merged_name, aggressive, max_parts_size_for_merge, can_merge);
        }
        else
        {
            selected = merger.selectAllPartsToMergeWithinPartition(parts, merged_name, disk_space, can_merge, partition, final);
        }

        if (!selected)
            return false;

        merging_tagger.emplace(parts, MergeTreeDataMerger::estimateDiskSpaceForMerge(parts), *this);
    }

    MergeList::EntryPtr merge_entry_ptr = context.getMergeList().insert(database_name, table_name, merged_name, merging_tagger->parts);

    /// Logging
    Stopwatch stopwatch;

    auto new_part = merger.mergePartsToTemporaryPart(
        merging_tagger->parts, merged_name, *merge_entry_ptr, aio_threshold, time(0), merging_tagger->reserved_space.get(), deduplicate);

    merger.renameMergedTemporaryPart(merging_tagger->parts, new_part, merged_name, nullptr);

    if (auto part_log = context.getPartLog(database_name, table_name))
    {
        PartLogElement elem;
        elem.event_time = time(nullptr);

        elem.merged_from.reserve(merging_tagger->parts.size());
        for (const auto & part : merging_tagger->parts)
            elem.merged_from.push_back(part->name);
        elem.event_type = PartLogElement::MERGE_PARTS;
        elem.size_in_bytes = new_part->size_in_bytes;

        elem.database_name = new_part->storage.getDatabaseName();
        elem.table_name = new_part->storage.getTableName();
        elem.part_name = new_part->name;

        elem.duration_ms = stopwatch.elapsed() / 1000000;

        part_log->add(elem);

        elem.duration_ms = 0;
        elem.event_type = PartLogElement::REMOVE_PART;
        elem.merged_from = Strings();

        for (const auto & part : merging_tagger->parts)
        {
            elem.part_name = part->name;
            elem.size_in_bytes = part->size_in_bytes;
            part_log->add(elem);
        }
    }

    return true;
}

bool StorageMergeTree::mergeTask()
{
    if (shutdown_called)
        return false;

    try
    {
        size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
        return merge(aio_threshold, false /*aggressive*/, {} /*partition*/, false /*final*/, false /*deduplicate*/); ///TODO: read deduplicate option from table config
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


void StorageMergeTree::clearColumnInPartition(const ASTPtr & query, const Field & partition, const Field & column_name, const Settings &)
{
    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger.cancel();

    auto lock_read_structure = lockStructure(false);
    auto lock_write_data = lockDataForAlter();

    String partition_id = MergeTreeData::getPartitionID(partition);
    MergeTreeData::DataParts parts = data.getDataParts();

    std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;

    AlterCommand alter_command;
    alter_command.type = AlterCommand::DROP_COLUMN;
    alter_command.column_name = get<String>(column_name);

    auto new_columns = data.getColumnsListNonMaterialized();
    auto new_materialized_columns = data.materialized_columns;
    auto new_alias_columns = data.alias_columns;
    auto new_column_defaults = data.column_defaults;

    alter_command.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

    auto columns_for_parts = new_columns;
    columns_for_parts.insert(std::end(columns_for_parts),
        std::begin(new_materialized_columns), std::end(new_materialized_columns));

    for (const auto & part : parts)
    {
        if (part->info.partition_id != partition_id)
            continue;

        if (auto transaction = data.alterDataPart(part, columns_for_parts, data.primary_expr_ast, false))
            transactions.push_back(std::move(transaction));

        LOG_DEBUG(log, "Removing column " << get<String>(column_name) << " from part " << part->name);
    }

    if (transactions.empty())
        return;

    for (auto & transaction : transactions)
        transaction->commit();

    data.recalculateColumnSizes();
}


void StorageMergeTree::dropPartition(const ASTPtr & query, const Field & partition, bool detach, const Settings & settings)
{
    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger.cancel();
    /// Waits for completion of merge and does not start new ones.
    auto lock = lockForAlter();

    String partition_id = MergeTreeData::getPartitionID(partition);

    size_t removed_parts = 0;
    MergeTreeData::DataParts parts = data.getDataParts();

    for (const auto & part : parts)
    {
        if (part->info.partition_id != partition_id)
            continue;

        LOG_DEBUG(log, "Removing part " << part->name);
        ++removed_parts;

        if (detach)
            data.renameAndDetachPart(part, "");
        else
            data.replaceParts({part}, {}, false);
    }

    LOG_INFO(log, (detach ? "Detached " : "Removed ") << removed_parts << " parts inside " << applyVisitor(FieldVisitorToString(), partition) << ".");
}


void StorageMergeTree::attachPartition(const ASTPtr & query, const Field & field, bool part, const Settings & settings)
{
    String partition;

    if (part)
        partition = field.getType() == Field::Types::UInt64 ? toString(field.get<UInt64>()) : field.safeGet<String>();
    else
        partition = MergeTreeData::getPartitionID(field);

    String source_dir = "detached/";

    /// Let's make a list of parts to add.
    Strings parts;
    if (part)
    {
        parts.push_back(partition);
    }
    else
    {
        LOG_DEBUG(log, "Looking for parts for partition " << partition << " in " << source_dir);
        ActiveDataPartSet active_parts;
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            String name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info)
                || part_info.partition_id != partition)
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
    context.resetCaches();
}


void StorageMergeTree::freezePartition(const Field & partition, const String & with_name, const Settings & settings)
{
    /// The prefix can be arbitrary. Not necessarily a month - you can specify only a year.
    data.freezePartition(partition.getType() == Field::Types::UInt64
        ? toString(partition.get<UInt64>())
        : partition.safeGet<String>(), with_name);
}

}
