#include <Storages/StorageInMemoryMetadata.h>

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>
#include <Common/quoteString.h>
#include <Core/ColumnWithTypeAndName.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TYPE_MISMATCH;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}

StorageInMemoryMetadata::StorageInMemoryMetadata(const StorageInMemoryMetadata & other)
    : columns(other.columns)
    , secondary_indices(other.secondary_indices)
    , constraints(other.constraints)
    , partition_key(other.partition_key)
    , primary_key(other.primary_key)
    , sorting_key(other.sorting_key)
    , sampling_key(other.sampling_key)
    , column_ttls_by_name(other.column_ttls_by_name)
    , table_ttl(other.table_ttl)
    , settings_changes(other.settings_changes ? other.settings_changes->clone() : nullptr)
    , select(other.select)
{
}

StorageInMemoryMetadata & StorageInMemoryMetadata::operator=(const StorageInMemoryMetadata & other)
{
    if (&other == this)
        return *this;

    columns = other.columns;
    secondary_indices = other.secondary_indices;
    constraints = other.constraints;
    partition_key = other.partition_key;
    primary_key = other.primary_key;
    sorting_key = other.sorting_key;
    sampling_key = other.sampling_key;
    column_ttls_by_name = other.column_ttls_by_name;
    table_ttl = other.table_ttl;
    if (other.settings_changes)
        settings_changes = other.settings_changes->clone();
    else
        settings_changes.reset();
    select = other.select;
    return *this;
}


void StorageInMemoryMetadata::setColumns(ColumnsDescription columns_)
{
    if (columns_.getAllPhysical().empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    columns = std::move(columns_);
}

void StorageInMemoryMetadata::setSecondaryIndices(IndicesDescription secondary_indices_)
{
    secondary_indices = std::move(secondary_indices_);
}

void StorageInMemoryMetadata::setConstraints(ConstraintsDescription constraints_)
{
    constraints = std::move(constraints_);
}

void StorageInMemoryMetadata::setTableTTLs(const TTLTableDescription & table_ttl_)
{
    table_ttl = table_ttl_;
}

void StorageInMemoryMetadata::setColumnTTLs(const TTLColumnsDescription & column_ttls_by_name_)
{
    column_ttls_by_name = column_ttls_by_name_;
}

void StorageInMemoryMetadata::setSettingsChanges(const ASTPtr & settings_changes_)
{
    if (settings_changes_)
        settings_changes = settings_changes_;
    else
        settings_changes = nullptr;
}

void StorageInMemoryMetadata::setSelectQuery(const SelectQueryDescription & select_)
{
    select = select_;
}

const ColumnsDescription & StorageInMemoryMetadata::getColumns() const
{
    return columns;
}

const IndicesDescription & StorageInMemoryMetadata::getSecondaryIndices() const
{
    return secondary_indices;
}

bool StorageInMemoryMetadata::hasSecondaryIndices() const
{
    return !secondary_indices.empty();
}

const ConstraintsDescription & StorageInMemoryMetadata::getConstraints() const
{
    return constraints;
}

TTLTableDescription StorageInMemoryMetadata::getTableTTLs() const
{
    return table_ttl;
}

bool StorageInMemoryMetadata::hasAnyTableTTL() const
{
    return hasAnyMoveTTL() || hasRowsTTL();
}

TTLColumnsDescription StorageInMemoryMetadata::getColumnTTLs() const
{
    return column_ttls_by_name;
}

bool StorageInMemoryMetadata::hasAnyColumnTTL() const
{
    return !column_ttls_by_name.empty();
}

TTLDescription StorageInMemoryMetadata::getRowsTTL() const
{
    return table_ttl.rows_ttl;
}

bool StorageInMemoryMetadata::hasRowsTTL() const
{
    return table_ttl.rows_ttl.expression != nullptr;
}

TTLDescriptions StorageInMemoryMetadata::getMoveTTLs() const
{
    return table_ttl.move_ttl;
}

bool StorageInMemoryMetadata::hasAnyMoveTTL() const
{
    return !table_ttl.move_ttl.empty();
}

ColumnDependencies StorageInMemoryMetadata::getColumnDependencies(const NameSet & updated_columns) const
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

Block StorageInMemoryMetadata::getSampleBlockNonMaterialized() const
{
    Block res;

    for (const auto & column : getColumns().getOrdinary())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockWithVirtuals(const NamesAndTypesList & virtuals) const
{
    auto res = getSampleBlock();

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    for (const auto & column : virtuals)
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block StorageInMemoryMetadata::getSampleBlock() const
{
    Block res;

    for (const auto & column : getColumns().getAllPhysical())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockForColumns(
    const Names & column_names, const NamesAndTypesList & virtuals, const StorageID & storage_id) const
{
    Block res;

    std::unordered_map<String, DataTypePtr> columns_map;

    NamesAndTypesList all_columns = getColumns().getAll();
    for (const auto & elem : all_columns)
        columns_map.emplace(elem.name, elem.type);

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    for (const auto & column : virtuals)
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
                "Column " + backQuote(name) + " not found in table " + storage_id.getNameForLogs(),
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }

    return res;
}

const KeyDescription & StorageInMemoryMetadata::getPartitionKey() const
{
    return partition_key;
}

bool StorageInMemoryMetadata::isPartitionKeyDefined() const
{
    return partition_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasPartitionKey() const
{
    return !partition_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForPartitionKey() const
{
    if (hasPartitionKey())
        return partition_key.expression->getRequiredColumns();
    return {};
}


const KeyDescription & StorageInMemoryMetadata::getSortingKey() const
{
    return sorting_key;
}

bool StorageInMemoryMetadata::isSortingKeyDefined() const
{
    return sorting_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasSortingKey() const
{
    return !sorting_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForSortingKey() const
{
    if (hasSortingKey())
        return sorting_key.expression->getRequiredColumns();
    return {};
}

Names StorageInMemoryMetadata::getSortingKeyColumns() const
{
    if (hasSortingKey())
        return sorting_key.column_names;
    return {};
}

const KeyDescription & StorageInMemoryMetadata::getSamplingKey() const
{
    return sampling_key;
}

bool StorageInMemoryMetadata::isSamplingKeyDefined() const
{
    return sampling_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasSamplingKey() const
{
    return !sampling_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForSampling() const
{
    if (hasSamplingKey())
        return sampling_key.expression->getRequiredColumns();
    return {};
}

const KeyDescription & StorageInMemoryMetadata::getPrimaryKey() const
{
    return primary_key;
}

bool StorageInMemoryMetadata::isPrimaryKeyDefined() const
{
    return primary_key.definition_ast != nullptr;
}

bool StorageInMemoryMetadata::hasPrimaryKey() const
{
    return !primary_key.column_names.empty();
}

Names StorageInMemoryMetadata::getColumnsRequiredForPrimaryKey() const
{
    if (hasPrimaryKey())
        return primary_key.expression->getRequiredColumns();
    return {};
}

Names StorageInMemoryMetadata::getPrimaryKeyColumns() const
{
    if (!primary_key.column_names.empty())
        return primary_key.column_names;
    return {};
}

ASTPtr StorageInMemoryMetadata::getSettingsChanges() const
{
    if (settings_changes)
        return settings_changes->clone();
    return nullptr;
}
const SelectQueryDescription & StorageInMemoryMetadata::getSelectQuery() const
{
    return select;
}

bool StorageInMemoryMetadata::hasSelectQuery() const
{
    return select.select_query != nullptr;
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

void StorageInMemoryMetadata::check(const Names & column_names, const NamesAndTypesList & virtuals, const StorageID & storage_id) const
{
    NamesAndTypesList available_columns = getColumns().getAllPhysical();
    available_columns.insert(available_columns.end(), virtuals.begin(), virtuals.end());

    const String list_of_columns = listOfColumns(available_columns);

    if (column_names.empty())
        throw Exception("Empty list of columns queried. There are columns: " + list_of_columns, ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    const auto columns_map = getColumnsMap(available_columns);

    auto unique_names = initUniqueStrings();
    for (const auto & name : column_names)
    {
        if (columns_map.end() == columns_map.find(name))
            throw Exception(
                "There is no column with name " + backQuote(name) + " in table " + storage_id.getNameForLogs() + ". There are columns: " + list_of_columns,
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}

void StorageInMemoryMetadata::check(const NamesAndTypesList & provided_columns) const
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

void StorageInMemoryMetadata::check(const NamesAndTypesList & provided_columns, const Names & column_names) const
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

void StorageInMemoryMetadata::check(const Block & block, bool need_all) const
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


}
