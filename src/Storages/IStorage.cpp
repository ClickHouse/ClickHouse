#include <Storages/IStorage.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/AlterCommands.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
}

bool IStorage::isVirtualColumn(const String & column_name, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Virtual column maybe overridden by real column
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
            type_str + " locking attempt on \"" + getStorageID().getFullTableName() + "\" has timed out! ("
                + std::to_string(acquire_timeout.count())
                + "ms) "
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

Pipe IStorage::read(
    const Names & /*column_names*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void IStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    auto pipe = read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    if (pipe.empty())
    {
        const auto & metadata_for_query = query_info.projection ? query_info.projection->desc->metadata : metadata_snapshot;
        auto header = getSampleBlockForColumns(metadata_for_query, column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, context);
    }
    else
    {
        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName());
        query_plan.addStep(std::move(read_step));
    }
}

Pipe IStorage::alterPartition(
    const StorageMetadataPtr & /* metadata_snapshot */, const PartitionCommands & /* commands */, ContextPtr /* context */)
{
    throw Exception("Partition operations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void IStorage::alter(const AlterCommands & params, ContextPtr context, TableLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void IStorage::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter())
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

void IStorage::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    throw Exception("Table engine " + getName() + " doesn't support mutations", ErrorCodes::NOT_IMPLEMENTED);
}

void IStorage::checkAlterPartitionIsPossible(
    const PartitionCommands & /*commands*/, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & /*settings*/) const
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

Names IStorage::getAllRegisteredNames() const
{
    Names result;
    auto getter = [](const auto & column) { return column.name; };
    const NamesAndTypesList & available_columns = getInMemoryMetadata().getColumns().getAllPhysical();
    std::transform(available_columns.begin(), available_columns.end(), std::back_inserter(result), getter);
    return result;
}

NameDependencies IStorage::getDependentViewsByColumn(ContextPtr context) const
{
    NameDependencies name_deps;
    auto dependencies = DatabaseCatalog::instance().getDependencies(storage_id);
    for (const auto & depend_id : dependencies)
    {
        auto depend_table = DatabaseCatalog::instance().getTable(depend_id, context);
        if (depend_table->getInMemoryMetadataPtr()->select.inner_query)
        {
            const auto & select_query = depend_table->getInMemoryMetadataPtr()->select.inner_query;
            auto required_columns = InterpreterSelectQuery(select_query, context, SelectQueryOptions{}.noModify()).getRequiredColumns();
            for (const auto & col_name : required_columns)
                name_deps[col_name].push_back(depend_id.table_name);
        }
    }
    return name_deps;
}

NamesAndTypesList IStorage::getColumns(const StorageMetadataPtr & metadata_snapshot, const GetColumnsOptions & options) const
{
    auto all_columns = metadata_snapshot->getColumns().get(options);

    if (options.with_virtuals)
    {
        /// Virtual columns must be appended after ordinary,
        /// because user can override them.
        auto virtuals = getVirtuals();
        if (!virtuals.empty())
        {
            NameSet column_names;
            for (const auto & column : all_columns)
                column_names.insert(column.name);
            for (auto && column : virtuals)
                if (!column_names.count(column.name))
                    all_columns.push_back(std::move(column));
        }
    }

    if (options.with_extended_objects)
        all_columns = extendObjectColumns(all_columns, options.with_subcolumns);

    return all_columns;
}

Block IStorage::getSampleBlockForColumns(const StorageMetadataPtr & metadata_snapshot, const Names & column_names) const
{
    Block res;

    auto all_columns = getColumns(
        metadata_snapshot,
        GetColumnsOptions(GetColumnsOptions::All)
            .withSubcolumns().withVirtuals().withExtendedObjects());

    std::unordered_map<String, DataTypePtr> columns_map;
    columns_map.reserve(all_columns.size());

    for (const auto & elem : all_columns)
        columns_map.emplace(elem.name, elem.type);

    for (const auto & name : column_names)
    {
        auto it = columns_map.find(name);
        if (it != columns_map.end())
            res.insert({it->second->createColumn(), it->second, it->first});
        else
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                "Column {} not found in table {}", backQuote(name), getStorageID().getNameForLogs());
    }

    return res;
}

void IStorage::check(const StorageMetadataPtr & metadata_snapshot, const Names & column_names) const
{
    auto available_columns = getColumns(
        metadata_snapshot,
        GetColumnsOptions(GetColumnsOptions::AllPhysical)
            .withSubcolumns().withVirtuals().withExtendedObjects()).getNames();

    if (column_names.empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}",
            boost::algorithm::join(available_columns, ","));

    std::unordered_set<std::string_view> columns_set(available_columns.begin(), available_columns.end());
    std::unordered_set<std::string_view> unique_names;

    for (const auto & name : column_names)
    {
        if (columns_set.end() == columns_set.find(name))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: ",
                backQuote(name), getStorageID().getNameForLogs(), boost::algorithm::join(available_columns, ","));

        if (unique_names.end() != unique_names.find(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}


std::string PrewhereDAGInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "PrewhereDagInfo\n";

    if (alias_actions)
    {
        ss << "alias_actions " << alias_actions->dumpDAG() << "\n";
    }

    if (prewhere_actions)
    {
        ss << "prewhere_actions " << prewhere_actions->dumpDAG() << "\n";
    }

    if (remove_columns_actions)
    {
        ss << "remove_columns_actions " << remove_columns_actions->dumpDAG() << "\n";
    }

    ss << "remove_prewhere_column " << remove_prewhere_column
       << ", need_filter " << need_filter << "\n";

    return ss.str();
}

std::string FilterDAGInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "FilterDAGInfo for column '" << column_name <<"', do_remove_column "
       << do_remove_column << "\n";
    if (actions)
    {
        ss << "actions " << actions->dumpDAG() << "\n";
    }

    return ss.str();
}

}
