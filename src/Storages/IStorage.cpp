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

const ColumnsDescription & IStorage::getColumns() const
{
    return metadata->columns;
}

namespace
{
#if !defined(ARCADIA_BUILD)
    using NamesAndTypesMap = google::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = google::dense_hash_set<StringRef, StringRefHash>;
#else
    using NamesAndTypesMap = google::sparsehash::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = google::sparsehash::dense_hash_set<StringRef, StringRefHash>;
#endif

    String listOfColumns(const NamesAndTypesList & available_columns)
    {
        std::stringstream ss;
        for (auto it = available_columns.begin(); it != available_columns.end(); ++it)
        {
            if (it != available_columns.begin())
                ss << ", ";
            ss << it->name;
        }
        return ss.str();
    }

    NamesAndTypesMap getColumnsMap(const NamesAndTypesList & columns)
    {
        NamesAndTypesMap res;
        res.set_empty_key(StringRef());

        for (const auto & column : columns)
            res.insert({column.name, column.type.get()});

        return res;
    }

    UniqueStrings initUniqueStrings()
    {
        UniqueStrings strings;
        strings.set_empty_key(StringRef());
        return strings;
    }
}

void IStorage::check(const Names & column_names, bool include_virtuals) const
{
    NamesAndTypesList available_columns = getColumns().getAllPhysical();
    if (include_virtuals)
    {
        auto virtuals = getVirtuals();
        available_columns.insert(available_columns.end(), virtuals.begin(), virtuals.end());
    }

    const String list_of_columns = listOfColumns(available_columns);

    if (column_names.empty())
        throw Exception("Empty list of columns queried. There are columns: " + list_of_columns, ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    const auto columns_map = getColumnsMap(available_columns);

    auto unique_names = initUniqueStrings();
    for (const auto & name : column_names)
    {
        if (columns_map.end() == columns_map.find(name))
            throw Exception(
                "There is no column with name " + backQuote(name) + " in table " + getStorageID().getNameForLogs() + ". There are columns: " + list_of_columns,
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}

void IStorage::check(const NamesAndTypesList & provided_columns) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    auto unique_names = initUniqueStrings();
    for (const NameAndTypePair & column : provided_columns)
    {
        auto it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception(
                "There is no column with name " + column.name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!column.type->equals(*it->second))
            throw Exception(
                "Type mismatch for column " + column.name + ". Column has type " + it->second->getName() + ", got type "
                    + column.type->getName(),
                ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(column.name))
            throw Exception("Column " + column.name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(column.name);
    }
}

void IStorage::check(const NamesAndTypesList & provided_columns, const Names & column_names) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto available_columns_map = getColumnsMap(available_columns);
    const auto & provided_columns_map = getColumnsMap(provided_columns);

    if (column_names.empty())
        throw Exception(
            "Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
            ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    auto unique_names = initUniqueStrings();
    for (const String & name : column_names)
    {
        auto it = provided_columns_map.find(name);
        if (provided_columns_map.end() == it)
            continue;

        auto jt = available_columns_map.find(name);
        if (available_columns_map.end() == jt)
            throw Exception(
                "There is no column with name " + name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!it->second->equals(*jt->second))
            throw Exception(
                "Type mismatch for column " + name + ". Column has type " + jt->second->getName() + ", got type " + it->second->getName(),
                ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}

void IStorage::check(const Block & block, bool need_all) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    NameSet names_in_block;

    block.checkNumberOfRows();

    for (const auto & column : block)
    {
        if (names_in_block.count(column.name))
            throw Exception("Duplicate column " + column.name + " in block", ErrorCodes::DUPLICATE_COLUMN);

        names_in_block.insert(column.name);

        auto it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception(
                "There is no column with name " + column.name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!column.type->equals(*it->second))
            throw Exception(
                "Type mismatch for column " + column.name + ". Column has type " + it->second->getName() + ", got type "
                    + column.type->getName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    if (need_all && names_in_block.size() < columns_map.size())
    {
        for (const auto & available_column : available_columns)
        {
            if (!names_in_block.count(available_column.name))
                throw Exception("Expected column " + available_column.name, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }
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

void IStorage::alter(
    const AlterCommands & params,
    const Context & context,
    TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
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

TTLTableDescription IStorage::getTableTTLs() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata->table_ttl;
}

bool IStorage::hasAnyTableTTL() const
{
    return hasAnyMoveTTL() || hasRowsTTL();
}

TTLColumnsDescription IStorage::getColumnTTLs() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata->column_ttls_by_name;
}

bool IStorage::hasAnyColumnTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return !metadata->column_ttls_by_name.empty();
}

TTLDescription IStorage::getRowsTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata->table_ttl.rows_ttl;
}

bool IStorage::hasRowsTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata->table_ttl.rows_ttl.expression != nullptr;
}

TTLDescriptions IStorage::getMoveTTLs() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata->table_ttl.move_ttl;
}

bool IStorage::hasAnyMoveTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return !metadata->table_ttl.move_ttl.empty();
}

ASTPtr IStorage::getSettingsChanges() const
{
    if (metadata->settings_changes)
        return metadata->settings_changes->clone();
    return nullptr;
}

const SelectQueryDescription & IStorage::getSelectQuery() const
{
    return metadata->select;
}

bool IStorage::hasSelectQuery() const
{
    return metadata->select.select_query != nullptr;
}

}
