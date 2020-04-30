#include <Storages/IStorage.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

#include <Storages/AlterCommands.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>

#include <Processors/Executors/TreeExecutorBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TYPE_MISMATCH;
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
}

IStorage::IStorage(StorageID storage_id_)
    : storage_id(std::move(storage_id_))
{
}

const ColumnsDescription & IStorage::getColumns() const
{
    return columns;
}

const IndicesDescription & IStorage::getIndices() const
{
    return indices;
}

const ConstraintsDescription & IStorage::getConstraints() const
{
    return constraints;
}

Block IStorage::getSampleBlock() const
{
    Block res;

    for (const auto & column : getColumns().getAllPhysical())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

void IStorage::setColumns(ColumnsDescription columns_)
{
    if (columns_.getOrdinary().empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    columns = std::move(columns_);
}

void IStorage::setIndices(IndicesDescription indices_)
{
    indices = std::move(indices_);
}

void IStorage::setConstraints(ConstraintsDescription constraints_)
{
    constraints = std::move(constraints_);
}

bool IStorage::isVirtualColumn(const String & column_name) const
{
    /// Virtual column maybe overriden by real column
    return !getColumns().has(column_name) && getVirtuals().contains(column_name);
}

RWLockImpl::LockHolder IStorage::tryLockTimed(
        const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const SettingSeconds & acquire_timeout) const
{
    auto lock_holder = rwlock->getLock(type, query_id, std::chrono::milliseconds(acquire_timeout.totalMilliseconds()));
    if (!lock_holder)
    {
        const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
        throw Exception(
                type_str + " locking attempt on \"" + getStorageID().getFullTableName() +
                "\" has timed out! (" + toString(acquire_timeout.totalMilliseconds()) + "ms) "
                "Possible deadlock avoided. Client should retry.",
                ErrorCodes::DEADLOCK_AVOIDED);
    }
    return lock_holder;
}

TableStructureReadLockHolder IStorage::lockStructureForShare(bool will_add_new_data, const String & query_id, const SettingSeconds & acquire_timeout)
{
    TableStructureReadLockHolder result;
    if (will_add_new_data)
        result.new_data_structure_lock = tryLockTimed(new_data_structure_lock, RWLockImpl::Read, query_id, acquire_timeout);
    result.structure_lock = tryLockTimed(structure_lock, RWLockImpl::Read, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
    return result;
}

TableStructureWriteLockHolder IStorage::lockAlterIntention(const String & query_id, const SettingSeconds & acquire_timeout)
{
    TableStructureWriteLockHolder result;
    result.alter_intention_lock = tryLockTimed(alter_intention_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
    return result;
}

void IStorage::lockStructureExclusively(TableStructureWriteLockHolder & lock_holder, const String & query_id, const SettingSeconds & acquire_timeout)
{
    if (!lock_holder.alter_intention_lock)
        throw Exception("Alter intention lock for table " + getStorageID().getNameForLogs() + " was not taken. This is a bug.", ErrorCodes::LOGICAL_ERROR);

    if (!lock_holder.new_data_structure_lock)
        lock_holder.new_data_structure_lock = tryLockTimed(new_data_structure_lock, RWLockImpl::Write, query_id, acquire_timeout);
    lock_holder.structure_lock = tryLockTimed(structure_lock, RWLockImpl::Write, query_id, acquire_timeout);
}

TableStructureWriteLockHolder IStorage::lockExclusively(const String & query_id, const SettingSeconds & acquire_timeout)
{
    TableStructureWriteLockHolder result;
    result.alter_intention_lock = tryLockTimed(alter_intention_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    result.new_data_structure_lock = tryLockTimed(new_data_structure_lock, RWLockImpl::Write, query_id, acquire_timeout);
    result.structure_lock = tryLockTimed(structure_lock, RWLockImpl::Write, query_id, acquire_timeout);

    return result;
}

StorageMetadataPtr IStorage::getInMemoryMetadata() const
{
    return std::make_shared<const StorageInMemoryMetadata>(getColumns(), getIndices(), getConstraints());
}

void IStorage::alter(
    const AlterCommands & params,
    const Context & context,
    TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto table_id = getStorageID();
    auto current_metadata = *getInMemoryMetadata();
    params.apply(current_metadata);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, current_metadata);
    setColumns(std::move(current_metadata.columns));
}


void IStorage::checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */)
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter())
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

BlockInputStreams IStorage::readStreams(
    const Names & column_names,
    const StorageMetadataPtr & metadata_version,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    ForceTreeShapedPipeline enable_tree_shape(query_info);
    auto pipes = read(column_names, metadata_version, query_info, context, processed_stage, max_block_size, num_streams);

    BlockInputStreams res;
    res.reserve(pipes.size());

    for (auto & pipe : pipes)
        res.emplace_back(std::make_shared<TreeExecutorBlockInputStream>(std::move(pipe)));

    return res;
}

StorageID IStorage::getStorageID() const
{
    std::lock_guard lock(id_mutex);
    return storage_id;
}

void IStorage::renameInMemory(const StorageID & new_table_id)
{
    std::lock_guard lock(id_mutex);
    storage_id = new_table_id;
}

NamesAndTypesList IStorage::getVirtuals() const
{
    return {};
}

}
