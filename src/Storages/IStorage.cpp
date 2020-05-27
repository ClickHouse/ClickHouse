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

StorageInMemoryMetadata IStorage::getInMemoryMetadata() const
{
    return StorageInMemoryMetadata(getColumns(), getIndices(), getConstraints());
}

void IStorage::alter(
    const AlterCommands & params,
    const Context & context,
    TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto table_id = getStorageID();
    StorageInMemoryMetadata metadata = getInMemoryMetadata();
    params.apply(metadata);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, metadata);
    setColumns(std::move(metadata.columns));
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
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    ForceTreeShapedPipeline enable_tree_shape(query_info);
    auto pipes = read(column_names, query_info, context, processed_stage, max_block_size, num_streams);

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

const StorageMetadataKeyField & IStorage::getPartitionKey() const
{
    return partition_key;
}

void IStorage::setPartitionKey(const StorageMetadataKeyField & partition_key_)
{
    partition_key = partition_key_;
}

bool IStorage::isPartitionKeyDefined() const
{
    return partition_key.definition_ast != nullptr;
}

bool IStorage::hasPartitionKey() const
{
    return !partition_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForPartitionKey() const
{
    if (hasPartitionKey())
        return partition_key.expression->getRequiredColumns();
    return {};
}

const StorageMetadataKeyField & IStorage::getSortingKey() const
{
    return sorting_key;
}

void IStorage::setSortingKey(const StorageMetadataKeyField & sorting_key_)
{
    sorting_key = sorting_key_;
}

bool IStorage::isSortingKeyDefined() const
{
    return sorting_key.definition_ast != nullptr;
}

bool IStorage::hasSortingKey() const
{
    return !sorting_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForSortingKey() const
{
    if (hasSortingKey())
        return sorting_key.expression->getRequiredColumns();
    return {};
}

Names IStorage::getSortingKeyColumns() const
{
    if (hasSortingKey())
        return sorting_key.column_names;
    return {};
}

const StorageMetadataKeyField & IStorage::getPrimaryKey() const
{
    return primary_key;
}

void IStorage::setPrimaryKey(const StorageMetadataKeyField & primary_key_)
{
    primary_key = primary_key_;
}

bool IStorage::isPrimaryKeyDefined() const
{
    return primary_key.definition_ast != nullptr;
}

bool IStorage::hasPrimaryKey() const
{
    return !primary_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForPrimaryKey() const
{
    if (hasPrimaryKey())
        return primary_key.expression->getRequiredColumns();
    return {};
}

Names IStorage::getPrimaryKeyColumns() const
{
    if (hasSortingKey())
        return primary_key.column_names;
    return {};
}

const StorageMetadataKeyField & IStorage::getSamplingKey() const
{
    return sampling_key;
}

void IStorage::setSamplingKey(const StorageMetadataKeyField & sampling_key_)
{
    sampling_key = sampling_key_;
}


bool IStorage::isSamplingKeyDefined() const
{
    return sampling_key.definition_ast != nullptr;
}

bool IStorage::hasSamplingKey() const
{
    return !sampling_key.column_names.empty();
}

Names IStorage::getColumnsRequiredForSampling() const
{
    if (hasSamplingKey())
        return sampling_key.expression->getRequiredColumns();
    return {};
}

}
