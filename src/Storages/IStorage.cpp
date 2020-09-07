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
    return metadata.columns;
}

const IndicesDescription & IStorage::getSecondaryIndices() const
{
    return metadata.secondary_indices;
}

bool IStorage::hasSecondaryIndices() const
{
    return !metadata.secondary_indices.empty();
}

const ConstraintsDescription & IStorage::getConstraints() const
{
    return metadata.constraints;
}

Block IStorage::getSampleBlock() const
{
    Block res;

    for (const auto & column : getColumns().getAllPhysical())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block IStorage::getSampleBlockWithVirtuals() const
{
    auto res = getSampleBlock();

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    for (const auto & column : getVirtuals())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block IStorage::getSampleBlockNonMaterialized() const
{
    Block res;

    for (const auto & column : getColumns().getOrdinary())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block IStorage::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    std::unordered_map<String, DataTypePtr> columns_map;

    NamesAndTypesList all_columns = getColumns().getAll();
    for (const auto & elem : all_columns)
        columns_map.emplace(elem.name, elem.type);

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    for (const auto & column : getVirtuals())
        columns_map.emplace(column.name, column.type);

    for (const auto & name : column_names)
    {
        auto it = columns_map.find(name);
        if (it != columns_map.end())
        {
            res.insert({it->second->createColumn(), it->second, it->first});
        }
        else
        {
            throw Exception(
                "Column " + backQuote(name) + " not found in table " + getStorageID().getNameForLogs(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }

    return res;
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

void IStorage::setColumns(ColumnsDescription columns_)
{
    if (columns_.getOrdinary().empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    metadata.columns = std::move(columns_);
}

void IStorage::setSecondaryIndices(IndicesDescription secondary_indices_)
{
    metadata.secondary_indices = std::move(secondary_indices_);
}

void IStorage::setConstraints(ConstraintsDescription constraints_)
{
    metadata.constraints = std::move(constraints_);
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
    setColumns(std::move(new_metadata.columns));
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

const KeyDescription & IStorage::getPartitionKey() const
{
    return metadata.partition_key;
}

void IStorage::setPartitionKey(const KeyDescription & partition_key_)
{
    metadata.partition_key = partition_key_;
}

bool IStorage::isPartitionKeyDefined() const
{
    return metadata.partition_key.definition_ast != nullptr;
}

bool IStorage::hasPartitionKey() const
{
    return !metadata.partition_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForPartitionKey() const
{
    if (hasPartitionKey())
        return metadata.partition_key.expression->getRequiredColumns();
    return {};
}

const KeyDescription & IStorage::getSortingKey() const
{
    return metadata.sorting_key;
}

void IStorage::setSortingKey(const KeyDescription & sorting_key_)
{
    metadata.sorting_key = sorting_key_;
}

bool IStorage::isSortingKeyDefined() const
{
    return metadata.sorting_key.definition_ast != nullptr;
}

bool IStorage::hasSortingKey() const
{
    return !metadata.sorting_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForSortingKey() const
{
    if (hasSortingKey())
        return metadata.sorting_key.expression->getRequiredColumns();
    return {};
}

Names IStorage::getSortingKeyColumns() const
{
    if (hasSortingKey())
        return metadata.sorting_key.column_names;
    return {};
}

const KeyDescription & IStorage::getPrimaryKey() const
{
    return metadata.primary_key;
}

void IStorage::setPrimaryKey(const KeyDescription & primary_key_)
{
    metadata.primary_key = primary_key_;
}

bool IStorage::isPrimaryKeyDefined() const
{
    return metadata.primary_key.definition_ast != nullptr;
}

bool IStorage::hasPrimaryKey() const
{
    return !metadata.primary_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForPrimaryKey() const
{
    if (hasPrimaryKey())
        return metadata.primary_key.expression->getRequiredColumns();
    return {};
}

Names IStorage::getPrimaryKeyColumns() const
{
    if (!metadata.primary_key.column_names.empty())
        return metadata.primary_key.column_names;
    return {};
}

const KeyDescription & IStorage::getSamplingKey() const
{
    return metadata.sampling_key;
}

void IStorage::setSamplingKey(const KeyDescription & sampling_key_)
{
    metadata.sampling_key = sampling_key_;
}


bool IStorage::isSamplingKeyDefined() const
{
    return metadata.sampling_key.definition_ast != nullptr;
}

bool IStorage::hasSamplingKey() const
{
    return !metadata.sampling_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForSampling() const
{
    if (hasSamplingKey())
        return metadata.sampling_key.expression->getRequiredColumns();
    return {};
}

TTLTableDescription IStorage::getTableTTLs() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata.table_ttl;
}

void IStorage::setTableTTLs(const TTLTableDescription & table_ttl_)
{
    std::lock_guard lock(ttl_mutex);
    metadata.table_ttl = table_ttl_;
}

bool IStorage::hasAnyTableTTL() const
{
    return hasAnyMoveTTL() || hasRowsTTL();
}

TTLColumnsDescription IStorage::getColumnTTLs() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata.column_ttls_by_name;
}

void IStorage::setColumnTTLs(const TTLColumnsDescription & column_ttls_by_name_)
{
    std::lock_guard lock(ttl_mutex);
    metadata.column_ttls_by_name = column_ttls_by_name_;
}

bool IStorage::hasAnyColumnTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return !metadata.column_ttls_by_name.empty();
}

TTLDescription IStorage::getRowsTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata.table_ttl.rows_ttl;
}

bool IStorage::hasRowsTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata.table_ttl.rows_ttl.expression != nullptr;
}

TTLDescriptions IStorage::getMoveTTLs() const
{
    std::lock_guard lock(ttl_mutex);
    return metadata.table_ttl.move_ttl;
}

bool IStorage::hasAnyMoveTTL() const
{
    std::lock_guard lock(ttl_mutex);
    return !metadata.table_ttl.move_ttl.empty();
}


ColumnDependencies IStorage::getColumnDependencies(const NameSet & updated_columns) const
{
    if (updated_columns.empty())
        return {};

    ColumnDependencies res;

    NameSet indices_columns;
    NameSet required_ttl_columns;
    NameSet updated_ttl_columns;

    auto add_dependent_columns = [&updated_columns](const auto & expression, auto & to_set)
    {
        auto requiered_columns = expression->getRequiredColumns();
        for (const auto & dependency : requiered_columns)
        {
            if (updated_columns.count(dependency))
            {
                to_set.insert(requiered_columns.begin(), requiered_columns.end());
                return true;
            }
        }

        return false;
    };

    for (const auto & index : getSecondaryIndices())
        add_dependent_columns(index.expression, indices_columns);

    if (hasRowsTTL())
    {
        auto rows_expression = getRowsTTL().expression;
        if (add_dependent_columns(rows_expression, required_ttl_columns))
        {
            /// Filter all columns, if rows TTL expression have to be recalculated.
            for (const auto & column : getColumns().getAllPhysical())
                updated_ttl_columns.insert(column.name);
        }
    }

    for (const auto & [name, entry] : getColumnTTLs())
    {
        if (add_dependent_columns(entry.expression, required_ttl_columns))
            updated_ttl_columns.insert(name);
    }

    for (const auto & entry : getMoveTTLs())
        add_dependent_columns(entry.expression, required_ttl_columns);

    for (const auto & column : indices_columns)
        res.emplace(column, ColumnDependency::SKIP_INDEX);
    for (const auto & column : required_ttl_columns)
        res.emplace(column, ColumnDependency::TTL_EXPRESSION);
    for (const auto & column : updated_ttl_columns)
        res.emplace(column, ColumnDependency::TTL_TARGET);

    return res;

}

ASTPtr IStorage::getSettingsChanges() const
{
    if (metadata.settings_changes)
        return metadata.settings_changes->clone();
    return nullptr;
}

void IStorage::setSettingsChanges(const ASTPtr & settings_changes_)
{
    if (settings_changes_)
        metadata.settings_changes = settings_changes_->clone();
    else
        metadata.settings_changes = nullptr;
}

const SelectQueryDescription & IStorage::getSelectQuery() const
{
    return metadata.select;
}

void IStorage::setSelectQuery(const SelectQueryDescription & select_)
{
    metadata.select = select_;
}

bool IStorage::hasSelectQuery() const
{
    return metadata.select.select_query != nullptr;
}

}
