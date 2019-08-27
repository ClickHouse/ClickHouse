#include <Storages/IStorage.h>

#include <Storages/AlterCommands.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TYPE_MISMATCH;
    extern const int SETTINGS_ARE_NOT_SUPPORTED;
    extern const int UNKNOWN_SETTING;
    extern const int TABLE_IS_DROPPED;
}

IStorage::IStorage(ColumnsDescription virtuals_) : virtuals(std::move(virtuals_))
{
}

const ColumnsDescription & IStorage::getColumns() const
{
    return columns;
}

const ColumnsDescription & IStorage::getVirtuals() const
{
    return virtuals;
}

const IndicesDescription & IStorage::getIndices() const
{
    return indices;
}

const ConstraintsDescription & IStorage::getConstraints() const
{
    return constraints;
}

NameAndTypePair IStorage::getColumn(const String & column_name) const
{
    /// By default, we assume that there are no virtual columns in the storage.
    return getColumns().getPhysical(column_name);
}

bool IStorage::hasColumn(const String & column_name) const
{
    /// By default, we assume that there are no virtual columns in the storage.
    return getColumns().hasPhysical(column_name);
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

    for (const auto & column : getColumns().getVirtuals())
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

    NamesAndTypesList all_columns = getColumns().getAll();
    std::unordered_map<String, DataTypePtr> columns_map;
    for (const auto & elem : all_columns)
        columns_map.emplace(elem.name, elem.type);

    for (const auto & name : column_names)
    {
        auto it = columns_map.find(name);
        if (it != columns_map.end())
        {
            res.insert({it->second->createColumn(), it->second, it->first});
        }
        else
        {
            /// Virtual columns.
            NameAndTypePair elem = getColumn(name);
            res.insert({elem.type->createColumn(), elem.type, elem.name});
        }
    }

    return res;
}

namespace
{
    using NamesAndTypesMap = GOOGLE_NAMESPACE::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = GOOGLE_NAMESPACE::dense_hash_set<StringRef, StringRefHash>;

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
        available_columns.splice(available_columns.end(), getColumns().getVirtuals());

    const String list_of_columns = listOfColumns(available_columns);

    if (column_names.empty())
        throw Exception("Empty list of columns queried. There are columns: " + list_of_columns, ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    const auto columns_map = getColumnsMap(available_columns);

    auto unique_names = initUniqueStrings();
    for (const auto & name : column_names)
    {
        if (columns_map.end() == columns_map.find(name))
            throw Exception(
                "There is no column with name " + backQuote(name) + " in table " + getTableName() + ". There are columns: " + list_of_columns,
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
        for (auto it = available_columns.begin(); it != available_columns.end(); ++it)
        {
            if (!names_in_block.count(it->name))
                throw Exception("Expected column " + it->name, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }
}

void IStorage::setColumns(ColumnsDescription columns_)
{
    if (columns_.getOrdinary().empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    columns = std::move(columns_);

    for (const auto & column : virtuals)
    {
        if (!columns.has(column.name))
            columns.add(column);
    }
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
    return getColumns().get(column_name).is_virtual;
}

bool IStorage::hasSetting(const String & /* setting_name */) const
{
    if (!supportsSettings())
        throw Exception("Storage '" + getName() + "' doesn't support settings.", ErrorCodes::SETTINGS_ARE_NOT_SUPPORTED);
    return false;
}

TableStructureReadLockHolder IStorage::lockStructureForShare(bool will_add_new_data, const String & query_id)
{
    TableStructureReadLockHolder result;
    if (will_add_new_data)
        result.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Read, query_id);
    result.structure_lock = structure_lock->getLock(RWLockImpl::Read, query_id);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
    return result;
}

TableStructureWriteLockHolder IStorage::lockAlterIntention(const String & query_id)
{
    TableStructureWriteLockHolder result;
    result.alter_intention_lock = alter_intention_lock->getLock(RWLockImpl::Write, query_id);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
    return result;
}

void IStorage::lockNewDataStructureExclusively(TableStructureWriteLockHolder & lock_holder, const String & query_id)
{
    if (!lock_holder.alter_intention_lock)
        throw Exception("Alter intention lock for table " + getTableName() + " was not taken. This is a bug.", ErrorCodes::LOGICAL_ERROR);

    lock_holder.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Write, query_id);
}

void IStorage::lockStructureExclusively(TableStructureWriteLockHolder & lock_holder, const String & query_id)
{
    if (!lock_holder.alter_intention_lock)
        throw Exception("Alter intention lock for table " + getTableName() + " was not taken. This is a bug.", ErrorCodes::LOGICAL_ERROR);

    if (!lock_holder.new_data_structure_lock)
        lock_holder.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Write, query_id);
    lock_holder.structure_lock = structure_lock->getLock(RWLockImpl::Write, query_id);
}

TableStructureWriteLockHolder IStorage::lockExclusively(const String & query_id)
{
    TableStructureWriteLockHolder result;
    result.alter_intention_lock = alter_intention_lock->getLock(RWLockImpl::Write, query_id);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    result.new_data_structure_lock = new_data_structure_lock->getLock(RWLockImpl::Write, query_id);
    result.structure_lock = structure_lock->getLock(RWLockImpl::Write, query_id);

    return result;
}


IDatabase::ASTModifier IStorage::getSettingsModifier(const SettingsChanges & new_changes) const
{
    return [&] (IAST & ast)
    {
        if (!new_changes.empty())
        {
            auto & storage_changes = ast.as<ASTStorage &>().settings->changes;
            /// Make storage settings unique
            for (const auto & change : new_changes)
            {
                if (hasSetting(change.name))
                {
                    auto finder = [&change] (const SettingChange & c) { return c.name == change.name; };
                    if (auto it = std::find_if(storage_changes.begin(), storage_changes.end(), finder); it != storage_changes.end())
                        it->value = change.value;
                    else
                        storage_changes.push_back(change);
                }
                else
                    throw Exception{"Storage '" + getName() + "' doesn't have setting '" + change.name + "'", ErrorCodes::UNKNOWN_SETTING};
            }
        }
    };
}


void IStorage::alter(
    const AlterCommands & params,
    const Context & context,
    TableStructureWriteLockHolder & table_lock_holder)
{
    if (params.isMutable())
        throw Exception("Method alter supports only change comment of column for storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);

    const String database_name = getDatabaseName();
    const String table_name = getTableName();

    if (params.isSettingsAlter())
    {
        SettingsChanges new_changes;
        params.applyForSettingsOnly(new_changes);
        IDatabase::ASTModifier settings_modifier = getSettingsModifier(new_changes);
        context.getDatabase(database_name)->alterTable(context, table_name, getColumns(), getIndices(), getConstraints(), settings_modifier);
    }
    else
    {
        lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());
        auto new_columns = getColumns();
        auto new_indices = getIndices();
        auto new_constraints = getConstraints();
        params.applyForColumnsOnly(new_columns);
        context.getDatabase(database_name)->alterTable(context, table_name, new_columns, new_indices, new_constraints, {});
        setColumns(std::move(new_columns));
    }
}

}
