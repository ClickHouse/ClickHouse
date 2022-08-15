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
    {
        auto table_id = getStorageID();
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped", table_id.database_name, table_id.table_name);
    }

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
        auto header = (query_info.projection ? query_info.projection->desc->metadata : metadata_snapshot)
                          ->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
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

std::string PrewhereInfo::dump() const
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
