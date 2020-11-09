#include <Storages/IStorage.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

#include <Storages/AlterCommands.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
}

bool IStorage::isVirtualColumn(const String & column_name, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Virtual column maybe overriden by real column
    return !metadata_snapshot->getColumns().has(column_name) && getVirtuals().contains(column_name);
}

RWLockImpl::LockHolder IStorage::tryLockTimed(
        const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const std::chrono::milliseconds & acquire_timeout) const
{
    auto lock_holder = rwlock->getLock(type, query_id, acquire_timeout);
    if (!lock_holder)
    {
        const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
        throw Exception(
                type_str + " locking attempt on \"" + getStorageID().getFullTableName() +
                "\" has timed out! (" + std::to_string(acquire_timeout.count()) + "ms) "
                "Possible deadlock avoided. Client should retry.",
                ErrorCodes::DEADLOCK_AVOIDED);
    }
    return lock_holder;
}

TableLockHolder IStorage::lockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLockImpl::Read, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    return result;
}

TableLockHolder IStorage::lockForAlter(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(alter_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    return result;
}


TableExclusiveLockHolder IStorage::lockExclusively(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableExclusiveLockHolder result;
    result.alter_lock = tryLockTimed(alter_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    result.drop_lock = tryLockTimed(drop_lock, RWLockImpl::Write, query_id, acquire_timeout);

    return result;
}

void IStorage::alter(
    const AlterCommands & params, const Context & context, TableLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void IStorage::checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter())
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

void IStorage::checkAlterPartitionIsPossible(const PartitionCommands & /*commands*/, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & /*settings*/) const
{
    throw Exception("Table engine " + getName() + " doesn't support partitioning", ErrorCodes::NOT_IMPLEMENTED);
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
