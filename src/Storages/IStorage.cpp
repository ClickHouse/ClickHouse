#include <Storages/IStorage.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Disks/IStoragePolicy.h>
#include <Common/CurrentThread.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Common/saturatedDuration.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/getEffectiveMaxThreads.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageValues.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Backups/RestorerFromBackup.h>
#include <Backups/IBackup.h>
#include <Planner/collectSelectedColumnsFromTable.h>

#include <algorithm>
#include <ranges>


namespace DB
{

namespace Setting
{
    extern const SettingsBool parallelize_output_from_storages;
    extern const SettingsBool distributed_aggregation_memory_efficient;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsNonZeroUInt64 max_distributed_connections;
}

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int TABLE_IS_BEING_RESTARTED;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

IStorage::IStorage(StorageID storage_id_, std::unique_ptr<StorageInMemoryMetadata> metadata_)
    : storage_id(std::move(storage_id_))
{
    if (metadata_)
        metadata.set(std::move(metadata_));
    else
        metadata.set(std::make_unique<StorageInMemoryMetadata>());
}

[[noreturn]] void IStorage::throwLockTimedOut(const RWLock & rwlock, RWLockImpl::Type type, const Poco::Timespan & acquire_timeout) const
{
    const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
    throw Exception(ErrorCodes::DEADLOCK_AVOIDED,
        "{} locking attempt on \"{}\" has timed out ({}ms) Possible deadlock avoided. Client should retry. Owner query ids: {}",
        type_str, getStorageID(), acquire_timeout.totalMilliseconds(), rwlock->getOwnerQueryIdsDescription());
}

RWLockImpl::LockHolder IStorage::tryLockTimed(
    const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const Poco::Timespan & acquire_timeout) const
{
    auto lock_holder = rwlock->getLock(type, query_id, std::chrono::milliseconds(acquire_timeout.totalMilliseconds()));
    if (!lock_holder)
        throwLockTimedOut(rwlock, type, acquire_timeout);
    return lock_holder;
}

RWLockImpl::LockHolder IStorage::tryLockTimedSliced(
    const RWLock & rwlock,
    RWLockImpl::Type type,
    const String & query_id,
    const Poco::Timespan & acquire_timeout,
    const std::function<bool()> & need_stop,
    const Poco::Timespan & check_period) const
{
    const auto total_timeout = std::chrono::milliseconds(acquire_timeout.totalMilliseconds());
    const auto slice = std::chrono::milliseconds(check_period.totalMilliseconds());
    const bool infinite_timeout = total_timeout == std::chrono::milliseconds::zero();

    if (slice <= std::chrono::milliseconds::zero())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The lock acquisition check period must be positive, got {} ms", slice.count());

    auto remaining = total_timeout;
    while (true)
    {
        /// `getLock` treats a zero timeout as an infinite wait, so the slice is never zero here:
        /// it is `slice` in the infinite case and at least 1 ms otherwise (`remaining` is positive).
        const auto attempt_timeout = infinite_timeout ? slice : std::min(remaining, slice);

        /// Unlike `tryLockTimed`, an expired slice is not an exception here: `getLock` returns a nullptr
        /// on a timeout, and the loop retries after polling `need_stop`.
        if (auto lock_holder = rwlock->getLock(type, query_id, attempt_timeout))
            return lock_holder;

        if (!infinite_timeout)
        {
            remaining -= attempt_timeout;
            if (remaining <= std::chrono::milliseconds::zero())
                throwLockTimedOut(rwlock, type, acquire_timeout);
        }

        if (need_stop())
            return nullptr;
    }
}

TableLockHolder IStorage::lockForShare(const String & query_id, const Poco::Timespan & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLockImpl::Read, query_id, acquire_timeout);
    auto table_id = getStorageID();
    if (!table_id.hasUUID() && (is_dropped || is_detached))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {}.{} is dropped or detached", table_id.database_name, table_id.table_name);

    if (is_being_restarted)
        throw Exception(
            ErrorCodes::TABLE_IS_BEING_RESTARTED, "Table {}.{} is being restarted", table_id.database_name, table_id.table_name);
    return result;
}

TableLockHolder IStorage::tryLockForShare(const String & query_id, const Poco::Timespan & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLockImpl::Read, query_id, acquire_timeout);

    auto table_id = getStorageID();
    if (is_being_restarted || (!table_id.hasUUID() && (is_dropped || is_detached)))
        // Table was dropped or is being restarted while acquiring the lock
        result = nullptr;
    return result;
}

TableLockHolder IStorage::tryLockForShare(
    const String & query_id,
    const Poco::Timespan & acquire_timeout,
    const std::function<bool()> & need_stop,
    const Poco::Timespan & check_period)
{
    TableLockHolder result = tryLockTimedSliced(drop_lock, RWLockImpl::Read, query_id, acquire_timeout, need_stop, check_period);
    if (!result)
        return nullptr;

    auto table_id = getStorageID();
    if (is_being_restarted || (!table_id.hasUUID() && (is_dropped || is_detached)))
        // Table was dropped or is being restarted while acquiring the lock
        result = nullptr;
    return result;
}

std::optional<IStorage::AlterLockHolder> IStorage::tryLockForAlter(const Poco::Timespan & acquire_timeout)
{
    AlterLockHolder lock{alter_lock, std::defer_lock};

    if (!lock.try_lock_for(saturatedMilliseconds(acquire_timeout.totalMilliseconds())))
        return {};

    if (is_dropped || is_detached)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", getStorageID());

    return lock;
}

IStorage::AlterLockHolder IStorage::lockForAlter(const Poco::Timespan & acquire_timeout)
{
    auto lock = tryLockForAlter(acquire_timeout);
    if (lock == std::nullopt)
        throw Exception(ErrorCodes::DEADLOCK_AVOIDED,
                        "Locking attempt for ALTER on \"{}\" has timed out ({} ms) "
                        "Possible deadlock avoided. Client should retry.",
                        getStorageID().getFullTableName(), acquire_timeout.totalMilliseconds());
    return std::move(*lock);
}


TableExclusiveLockHolder IStorage::lockExclusively(const String & query_id, const Poco::Timespan & acquire_timeout)
{
    TableExclusiveLockHolder result;
    result.drop_lock = tryLockTimed(drop_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped || is_detached)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", getStorageID());

    return result;
}

Pipe IStorage::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method read is not supported by storage {}", getName());
}

SinkToStoragePtr IStorage::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr /*context*/,
    bool /*async_insert*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is not supported by storage {}", getName());
}

void IStorage::truncate(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /* metadata_snapshot */,
    ContextPtr /* context */,
    TableExclusiveLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Truncate is not supported by storage {}", getName());
}

void IStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto pipe = read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    /// parallelize processing if not yet
    const size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef()[Setting::parallelize_output_from_storages];

    /// For distributed_aggregation_memory_efficient with Two-Level-Hash aggregation, the `GroupingAggregatedTransform`
    /// need to receive buckets from Remote in order of bucket number, while resize here will break the buckets order
    /// return from `RemoteSource`. See https://github.com/ClickHouse/ClickHouse/issues/76934.
    const bool should_not_resize = context->getSettingsRef()[Setting::distributed_aggregation_memory_efficient]
        && processed_stage == QueryProcessingStage::Enum::WithMergeableState;

    /// `num_streams` is a read-parallelism request, not a thread budget: the resize must not create more
    /// output ports than there are threads to consume them. That budget is the one the plan runs with, which
    /// `InterpreterSelectQuery` and `PlannerJoinTree` compute as `max_threads` - except for a synchronous
    /// remote read, where a thread blocks on a socket instead of running and they raise it to
    /// `max_distributed_connections` (and pass it to `QueryPlan::setMaxThreads`). Make the same choice here,
    /// so that such a read keeps the fan-out it asked for.
    ///
    /// This resize is picked while the query plan is being built, where there is no
    /// `BuildQueryPipelineSettings` to take the budget from, hence the direct call to
    /// `getEffectiveMaxThreads`. The other three post-read resizes are picked in
    /// `initializePipeline` and read `BuildQueryPipelineSettings::max_threads`, which is this same helper
    /// applied to the same settings.
    const auto & settings = context->getSettingsRef();
    const size_t max_threads_execute_query = isRemote() && !settings[Setting::async_socket_for_remote]
        ? settings[Setting::max_distributed_connections]
        : getEffectiveMaxThreads(settings);
    const size_t resize_to = std::min(num_streams, max_threads_execute_query);

    if (!should_not_resize && parallelize_output && parallelizeOutputAfterReading(context) && output_ports > 0
        && output_ports < resize_to)
        pipe.resize(resize_to);

    readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, context, shared_from_this());
}

void IStorage::readFromPipe(
    QueryPlan & query_plan,
    Pipe pipe,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    std::shared_ptr<IStorage> storage_)
{
    if (pipe.empty())
    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info);
    }
    else
    {
        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), storage_, context, query_info);
        query_plan.addStep(std::move(read_step));
    }
}

std::optional<QueryPipeline> IStorage::distributedWrite(const ASTInsertQuery & /*query*/, ContextPtr /*context*/)
{
    return {};
}

Pipe IStorage::alterPartition(
    const StorageMetadataPtr & /* metadata_snapshot */, const PartitionCommands & /* commands */, ContextPtr /* context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition operations are not supported by storage {}", getName());
}

void IStorage::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder &, DDLGuardPtr &)
{
    auto table_id = getStorageID();
    auto storage_metadata_snapshot = getInMemoryMetadataPtr(context, false);
    StorageInMemoryMetadata new_metadata = *storage_metadata_snapshot;
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}

void IStorage::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());
    }
}

void IStorage::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support mutations", getName());
}

void IStorage::checkAlterPartitionIsPossible(
    const PartitionCommands & /*commands*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const Settings & /*settings*/,
    ContextPtr /*context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support partitioning", getName());
}

bool IStorage::optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Names & /* deduplicate_by_columns */,
        bool /*cleanup*/,
        ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method optimize is not supported by storage {}", getName());
}

std::expected<void, PreformattedMessage> IStorage::supportsLightweightUpdate() const
{
    return std::unexpected(PreformattedMessage::create("Table with engine {} doesn't support lightweight updates", getName()));
}

QueryPipeline IStorage::updateLightweight(const MutationCommands &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Lightweight updates are not supported by storage {}", getName());
}

void IStorage::mutate(const MutationCommands &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mutations are not supported by storage {}", getName());
}

Pipe IStorage::executeCommand(const String & command_name, const ASTPtr & /*args*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXECUTE command '{}' is not supported by storage {}", command_name, getName());
}

CancellationCode IStorage::killMutation(const String & /*mutation_id*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mutations are not supported by storage {}", getName());
}

void IStorage::waitForMutation(const String & /*mutation_id*/, bool /*wait_for_another_mutation*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mutations are not supported by storage {}", getName());
}

void IStorage::setMutationCSN(const String & /*mutation_id*/, UInt64 /*csn*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Mutations are not supported by storage {}", getName());
}

CancellationCode IStorage::killPartMoveToShard(const UUID & /*task_uuid*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Part moves between shards are not supported by storage {}", getName());
}

StorageID IStorage::getStorageID() const
{
    std::lock_guard lock(id_mutex);
    return storage_id;
}

bool IStorage::supportsSampling() const
{
    auto storage_metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    return storage_metadata_snapshot->hasSamplingKey();
}

ConditionSelectivityEstimatorPtr IStorage::getConditionSelectivityEstimator(const RangesInDataParts &, const Names &, ContextPtr) const
{
    return nullptr;
}

void IStorage::renameInMemory(const StorageID & new_table_id)
{
    std::lock_guard lock(id_mutex);
    storage_id = new_table_id;
}

bool IStorage::hasUniqueKey() const
{
    return metadata.get()->hasUniqueKey();
}

VectorWithMemoryTracking<String> IStorage::getAllRegisteredNames() const
{
    VectorWithMemoryTracking<String> result;
    auto getter = [](const auto & column) { return column.name; };
    const auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    const auto & available_columns = metadata_snapshot->getColumns().getAllPhysical();
    std::transform(available_columns.begin(), available_columns.end(), std::back_inserter(result), getter);
    return result;
}

NameDependencies IStorage::getDependentViewsByColumn(ContextPtr context) const
{
    NameDependencies name_deps;
    auto current_storage_id = getStorageID();
    auto view_ids = DatabaseCatalog::instance().getDependentViews(current_storage_id);
    for (const auto & view_id : view_ids)
    {
        auto view = DatabaseCatalog::instance().getTable(view_id, context);
        auto view_metadata = view->getInMemoryMetadataPtr(context, false);
        if (view_metadata->select.inner_query)
        {
            const auto & select_query = view_metadata->select.inner_query;
            auto interpreter = InterpreterSelectQueryAnalyzer(select_query, context, SelectQueryOptions{}.noModify());
            auto query_tree = interpreter.getQueryTree();
            Names required_columns = collectSelectedColumnsFromTable(query_tree, current_storage_id, context);

            for (const auto & col_name : required_columns)
                name_deps[col_name].push_back(view_id.table_name);
        }
    }
    return name_deps;
}

namespace
{

/// So that a scalar subquery is kept as the source expression of the constant it folds into, not hashed into `__getScalar`.
void disableScalarSubqueryOptimization(const QueryTreeNodePtr & node)
{
    ContextMutablePtr * context = nullptr;
    if (auto * query_node = node->as<QueryNode>())
        context = &query_node->getMutableContext();
    else if (auto * union_node = node->as<UnionNode>())
        context = &union_node->getMutableContext();

    if (context)
    {
        *context = Context::createCopy(*context);
        (*context)->setSetting("enable_scalar_subquery_optimization", Field(false));
    }

    for (const auto & child : node->getChildren())
        if (child)
            disableScalarSubqueryOptimization(child);
}

/// The view's query analyzed as an `INSERT` does for rows pushed from a source with `source_metadata`, or over the stored source.
QueryTreeNodePtr analyzeViewQueryForSource(
    const ASTPtr & query, const StorageID & source_id, const StorageInMemoryMetadata * source_metadata, ContextPtr context)
{
    auto view_context = Context::createCopy(context);
    StoragePtr view_source;
    if (source_metadata)
    {
        view_source = std::make_shared<StorageValues>(
            source_id, source_metadata->getColumns(), source_metadata->getSampleBlock(), source_metadata->virtuals);
        view_context->addViewSource(view_source);
    }

    auto query_tree = buildQueryTree(query, view_context);
    disableScalarSubqueryOptimization(query_tree);
    QueryAnalysisPass analysis(
        /*only_analyze_=*/ true, /*positional_arguments_already_resolved_=*/ false, view_source, /*view_source_in_subqueries_=*/ true);
    analysis.run(query_tree, view_context);
    return query_tree;
}

/// Replaces the constants that analysis folded with the expressions they come from, which `isEqual` does not compare, and renames
/// the column `from` of the source to `to`, as a rename of the source column alone would change the tree.
void prepareForComparison(QueryTreeNodePtr & node, const StorageID & source_id, const String & from, const String & to)
{
    while (node->getNodeType() == QueryTreeNodeType::CONSTANT && node->as<ConstantNode &>().hasSourceExpression())
        node = node->as<ConstantNode &>().getSourceExpression();

    if (const auto * column = node->as<ColumnNode>(); column && !from.empty() && column->getColumnName() == from)
    {
        auto source = column->getColumnSourceOrNull();
        if (const auto * table = source ? source->as<TableNode>() : nullptr; table && table->getStorageID() == source_id)
        {
            NameAndTypePair renamed{to, column->getColumnType()};
            if (column->hasExpression())
                node = std::make_shared<ColumnNode>(std::move(renamed), column->getExpression(), source);
            else
                node = std::make_shared<ColumnNode>(std::move(renamed), source);
        }
    }

    for (auto & child : node->getChildren())
        if (child)
            prepareForComparison(child, source_id, from, to);
}

/// Gives the recursive CTEs of `node` the temporary tables of the same CTEs in `like`, as each analysis creates new ones.
void adoptRecursiveCTETables(
    const QueryTreeNodePtr & node, const QueryTreeNodePtr & like, std::vector<std::pair<StorageID, StoragePtr>> & adopted, ContextPtr context)
{
    if (node->getNodeType() != like->getNodeType())
        return;

    auto * union_node = node->as<UnionNode>();
    const auto * like_union_node = like->as<UnionNode>();
    const bool adopt = union_node && union_node->hasRecursiveCTETable() && like_union_node->hasRecursiveCTETable()
        && union_node->getRecursiveCTETable()->columns == like_union_node->getRecursiveCTETable()->columns;
    if (adopt)
        adopted.emplace_back(union_node->getRecursiveCTETable()->getStorageID(), like_union_node->getRecursiveCTETable()->storage);

    if (auto * table = node->as<TableNode>())
    {
        for (const auto & [storage_id, storage] : adopted)
        {
            if (table->getStorageID() == storage_id)
            {
                table->updateStorage(storage, context);
                break;
            }
        }
    }

    const auto & children = node->getChildren();
    const auto & like_children = like->getChildren();
    if (children.size() == like_children.size())
        for (size_t i = 0; i < children.size(); ++i)
            if (children[i] && like_children[i])
                adoptRecursiveCTETables(children[i], like_children[i], adopted, context);

    if (adopt)
        union_node->setRecursiveCTETable(*like_union_node->getRecursiveCTETable());
}

std::vector<std::string_view> splitNameParts(std::string_view name)
{
    std::vector<std::string_view> parts;
    for (auto part : std::views::split(name, '.'))
        parts.emplace_back(part.begin(), part.end());
    return parts;
}

/// Whether `ast` may read the column with the name `column_parts`: by a matcher such as `*`, or by an identifier that, from
/// some part on, names the column, the `Nested` it belongs to, or a subcolumn of it.
bool mayReadColumn(const IAST & ast, const std::vector<std::string_view> & column_parts)
{
    if (ast.as<ASTAsterisk>() || ast.as<ASTQualifiedAsterisk>() || ast.as<ASTColumnsRegexpMatcher>()
        || ast.as<ASTQualifiedColumnsRegexpMatcher>() || ast.as<ASTColumnsListMatcher>() || ast.as<ASTQualifiedColumnsListMatcher>())
        return true;

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto parts = splitNameParts(identifier->name());
        for (size_t i = 0; i < parts.size(); ++i)
        {
            const size_t length = std::min(parts.size() - i, column_parts.size());
            if (std::equal(parts.begin() + i, parts.begin() + i + length, column_parts.begin()))
                return true;
        }
    }

    return std::ranges::any_of(ast.children, [&](const auto & child) { return mayReadColumn(*child, column_parts); });
}

}

void IStorage::checkAlterKeepsDependentViews(
    const AlterCommands & commands, const AlterCommand & command, ContextPtr context, bool share_nested_offsets) const
{
    const bool is_drop = command.type == AlterCommand::DROP_COLUMN && !command.clear;
    if (command.ignore || (!is_drop && command.type != AlterCommand::RENAME_COLUMN))
        return;

    /// A replica applies what the initiator committed, even if it has views the initiator does not.
    if (isSecondaryDDLReplay(context))
        return;

    const auto source_id = getStorageID();
    std::vector<std::pair<StorageID, StorageMetadataHandle>> views;
    for (const auto & view_id : DatabaseCatalog::instance().getDependentViews(source_id))
    {
        auto view = DatabaseCatalog::instance().getTable(view_id, context);
        auto view_metadata = view->getInMemoryMetadataPtr(context, false);
        if (view_metadata->select.inner_query)
            views.emplace_back(view_id, std::move(view_metadata));
    }
    if (views.empty())
        return;

    /// The metadata as the commands of the ALTER before `command` leave it.
    const auto current_metadata = getInMemoryMetadataPtr(context, false);
    auto old_metadata = *current_metadata;
    for (const auto & previous : commands)
    {
        if (&previous == &command)
            break;
        if (!previous.ignore)
            previous.apply(old_metadata, context, share_nested_offsets);
    }
    auto new_metadata = old_metadata;
    command.apply(new_metadata, context, share_nested_offsets);

    Strings broken_views;
    for (const auto & [view_id, view_metadata] : views)
    {
        const auto & query = view_metadata->select.inner_query;
        auto view_context = view_metadata->getSQLSecurityOverriddenContext(context);
        QueryTreeNodePtr query_tree;
        try
        {
            query_tree = analyzeViewQueryForSource(query, source_id, &old_metadata, view_context);
        }
        catch (const Exception &)
        {
            /// E.g. a table function argument computed from the source can be undefined for no rows. If the query works on the
            /// stored rows, whether it names the column, or the new name it may then resolve to, decides.
            analyzeViewQueryForSource(query, source_id, nullptr, view_context);
            if (mayReadColumn(*query, splitNameParts(command.column_name))
                || (!is_drop && mayReadColumn(*query, splitNameParts(command.rename_to))))
                broken_views.push_back(view_id.table_name);
            continue;
        }
        prepareForComparison(query_tree, source_id, is_drop ? "" : command.column_name, command.rename_to);

        /// The view is kept only if its query resolves every name as before, e.g. a renamed column's name not to an alias.
        try
        {
            auto new_query_tree = analyzeViewQueryForSource(query, source_id, &new_metadata, view_context);
            prepareForComparison(new_query_tree, source_id, "", "");
            std::vector<std::pair<StorageID, StoragePtr>> adopted;
            adoptRecursiveCTETables(new_query_tree, query_tree, adopted, view_context);
            if (new_query_tree->isEqual(*query_tree))
                continue;
        }
        catch (const Exception &)
        {
            tryLogCurrentException(
                __func__, fmt::format("The query of view {} fails to analyze after the ALTER", view_id.getNameForLogs()), LogsLevel::debug);
        }
        broken_views.push_back(view_id.table_name);
    }

    if (!broken_views.empty())
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
            "Trying to ALTER {} column {} which is referenced by materialized view {}",
            is_drop ? "DROP" : "RENAME", backQuoteIfNeed(command.column_name), toString(broken_views));
}

bool IStorage::isStaticStorage() const
{
    auto storage_policy = getStoragePolicy();
    if (storage_policy)
    {
        for (const auto & disk : storage_policy->getDisks())
            if (!(disk->isReadOnly() || disk->isWriteOnce()))
                return false;
        return true;
    }
    return false;
}

IStorage::DataValidationTasksPtr IStorage::getCheckTaskList(const CheckTaskFilter &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Check query is not supported for {} storage", getName());
}

std::optional<CheckResult> IStorage::checkDataNext(DataValidationTasksPtr & /* check_task_list */)
{
    return {};
}

void IStorage::applyMetadataChangesToCreateQueryForBackup(const ASTPtr &) const
{
}

void IStorage::backupData(BackupEntriesCollector &, const String &, const std::optional<ASTs> &)
{
}

void IStorage::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    /// If an inherited class doesn't override restoreDataFromBackup() that means it doesn't backup any data.
    auto filenames = restorer.getBackup()->listFiles(data_path_in_backup, /*recursive*/ false);
    if (!filenames.empty())
        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore table {}: Folder {} in backup must be empty",
                        getStorageID().getFullTableName(), data_path_in_backup);
}

std::string PrewhereInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "PrewhereDagInfo\n";

    {
        ss << "prewhere_actions " << prewhere_actions.dumpDAG() << "\n";
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

    ss << "actions " << actions.dumpDAG() << "\n";

    return ss.str();
}

}
