#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Storages/System/extractTablesFilter.h>
#include <Common/escapeForFileName.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/StorageProxy.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/DatabaseCatalog.h>

namespace
{
constexpr auto * database_column_name = "database";
constexpr auto * table_column_name = "table";
constexpr auto * engine_column_name = "engine";
constexpr auto * active_column_name = "active";
constexpr auto * storage_uuid_column_name = "storage_uuid";
}

namespace ProfileEvents
{
    extern const Event SystemPartsEnumerationSlowdownSleeps;
}

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 preferred_block_size_bytes;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char slowdown_system_parts_enumeration[];
}

void slowDownSystemPartsEnumeration([[maybe_unused]] const String & table_name)
{
    fiu_do_on(FailPoints::slowdown_system_parts_enumeration,
    {
        /// Slow down only the tables with a special name prefix, so that the tests using
        /// this failpoint do not affect concurrent queries over the tables of other tests.
        if (table_name.starts_with("t_slowdown_system_parts"))
        {
            ProfileEvents::increment(ProfileEvents::SystemPartsEnumerationSlowdownSleeps);
            sleepForMilliseconds(500);
        }
    });
}

void slowDownSystemPartsColumnsEnumeration([[maybe_unused]] const String & table_name, [[maybe_unused]] size_t column_position)
{
    fiu_do_on(FailPoints::slowdown_system_parts_enumeration,
    {
        /// Sleep at the same cadence as the cancellation checkpoints of the column-enumeration
        /// loops, so that a test can prove by counting the sleeps that those checkpoints stop
        /// the eager result building on a table with many columns per part.
        if (column_position % COLUMNS_CANCELLATION_CHECK_PERIOD == 0 && table_name.starts_with("t_slowdown_system_parts"))
        {
            ProfileEvents::increment(ProfileEvents::SystemPartsEnumerationSlowdownSleeps);
            sleepForMilliseconds(500);
        }
    });
}

void slowDownSystemPartsDiscovery([[maybe_unused]] const String & table_name)
{
    fiu_do_on(FailPoints::slowdown_system_parts_enumeration,
    {
        /// Sleep on every walked table of the storage-discovery prepass, so that a test can prove
        /// by counting the sleeps that the cancellation checkpoint of the walk stops it. The
        /// narrower name prefix keeps the walk fast for the tables that test the later
        /// per-part / per-column checkpoints.
        if (table_name.starts_with("t_slowdown_system_parts_discovery"))
        {
            ProfileEvents::increment(ProfileEvents::SystemPartsEnumerationSlowdownSleeps);
            sleepForMilliseconds(500);
        }
    });
}

void slowDownSystemPartsMetadataEnumeration([[maybe_unused]] const String & table_name, [[maybe_unused]] size_t column_position)
{
    fiu_do_on(FailPoints::slowdown_system_parts_enumeration,
    {
        /// Sleep at the same cadence as the cancellation checkpoints of the column-metadata
        /// prepasses, so that a test can prove by counting the sleeps that those checkpoints
        /// stop the prepass on a table with many columns. The narrower name prefix keeps the
        /// prepass fast for the tables that test the later per-part / per-column checkpoints.
        if (column_position % COLUMNS_CANCELLATION_CHECK_PERIOD == 0 && table_name.starts_with("t_slowdown_system_parts_meta"))
        {
            ProfileEvents::increment(ProfileEvents::SystemPartsEnumerationSlowdownSleeps);
            sleepForMilliseconds(500);
        }
    });
}

StoragesInfoStreamBase::StoragesInfoStreamBase(ContextPtr context)
    : query_id(context->getCurrentQueryId()), lock_timeout(std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds())), query_status(context->getProcessListElement()), next_row(0), rows(0)
{
}

StoragesInfo StoragesInfoStreamBase::next()
{
    while (next_row < rows)
    {
        /// The check is needed because if many tables were dropped concurrently,
        /// this loop can spend a long time skipping them, retaking the locks.
        /// Note: in the 'break' mode `checkTimeLimit` returns false instead of throwing, and the
        /// enumeration stops here rather than walking the remaining storages.
        if (query_status && !query_status->checkTimeLimit())
            return {};

        StoragesInfo info;

        info.database = (*database_column)[next_row].safeGet<String>();
        info.table = (*table_column)[next_row].safeGet<String>();
        UUID storage_uuid = (*storage_uuid_column)[next_row].safeGet<UUID>();

        auto is_same_table = [&storage_uuid, this] (size_t row) -> bool
        {
            return (*storage_uuid_column)[row].safeGet<UUID>() == storage_uuid;
        };

        /// We may have two rows per table which differ in 'active' value.
        /// If rows with 'active = 0' were not filtered out, this means we
        /// must collect the inactive parts. Remember this fact in StoragesInfo.
        for (; next_row < rows && is_same_table(next_row); ++next_row)
        {
            const auto active = (*active_column)[next_row].safeGet<UInt64>();
            if (active == 0)
                info.need_inactive_parts = true;
        }

        info.storage = storages.at(storage_uuid);

        /// For table not to be dropped and set of columns to remain constant.
        if (!tryLockTable(info))
            continue;

        info.engine = info.storage->getName();

        info.data = castStorage<MergeTreeData>(info.storage, DeferredTable::Skip).get();
        if (!info.data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown engine {}", info.engine);

        return info;
    }

    return {};
}

bool StorageSystemPartsBase::hasStateColumn(const Names & column_names, const StorageSnapshotPtr & storage_snapshot)
{
    bool has_state_column = false;
    Names real_column_names;

    for (const String & column_name : column_names)
    {
        if (column_name == "_state")
            has_state_column = true;
        else
            real_column_names.emplace_back(column_name);
    }

    /// Do not check if only _state column is requested
    if (!(has_state_column && real_column_names.empty()))
        storage_snapshot->check(real_column_names);

    return has_state_column;
}

namespace
{

/// A callback for the part enumeration in MergeTreeData to stop it on query cancellation or a time limit.
/// checkTimeLimit returns false (instead of throwing) only in the 'break' overflow mode, where an
/// enumeration that stops early is consistent with the semantics of the mode.
std::function<bool()> makeNeedStopCallback(const QueryStatusPtr & query_status)
{
    if (!query_status)
        return {};

    return [query_status] { return !query_status->checkTimeLimit(); };
}

}

MergeTreeData::DataPartsVector
StoragesInfo::getParts(MergeTreeData::DataPartStateVector & state, bool has_state_column, const std::shared_ptr<QueryStatus> & query_status) const
{
    using State = MergeTreeData::DataPartState;
    using Kind = MergeTreeData::DataPartKind;

    auto need_stop = makeNeedStopCallback(query_status);

    if (need_inactive_parts)
    {
        /// If has_state_column is requested, return all states.
        if (!has_state_column)
            return data->getDataPartsVectorForInternalUsage({State::Active, State::Outdated}, {Kind::Regular, Kind::Patch}, &state, need_stop);

        return data->getAllDataPartsVector(&state, need_stop);
    }

    return data->getDataPartsVectorForInternalUsage({State::Active}, {Kind::Regular, Kind::Patch}, &state, need_stop);
}

MergeTreeData::ProjectionPartsVector
StoragesInfo::getProjectionParts(MergeTreeData::DataPartStateVector & state, bool has_state_column, const std::shared_ptr<QueryStatus> & query_status) const
{
    auto metadata_snapshot = data->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    if (metadata_snapshot->projections.empty())
        return {};

    auto need_stop = makeNeedStopCallback(query_status);

    using State = MergeTreeData::DataPartState;
    if (need_inactive_parts)
    {
        /// If has_state_column is requested, return all states.
        if (!has_state_column)
            return data->getProjectionPartsVectorForInternalUsage({State::Active, State::Outdated}, &state, need_stop);

        return data->getAllProjectionPartsVector(&state, need_stop);
    }

    return data->getProjectionPartsVectorForInternalUsage({State::Active}, &state, need_stop);
}

StoragesInfoStream::StoragesInfoStream(std::optional<ActionsDAG> filter_by_database, std::optional<ActionsDAG> filter_by_other_columns, ContextPtr context)
    : StoragesInfoStreamBase(context)
{
    /// Will apply WHERE to subset of columns and then add more columns.
    /// This is kind of complicated, but we use WHERE to do less work.

    Block block_to_filter;

    /// A condition on `table` prunes the enumeration below, which resolves every storage of
    /// every database that survived the condition on `database`. The sets of `filter_by_other_columns`
    /// were built in `applyFilters` with their elements kept, so an `IN` over a subquery is readable
    /// here too. A condition that names the database together with the table, such as
    /// `(database, table) IN (...)`, cannot be split off into `filter_by_database`; the exact
    /// database names are read from the same filter so such a condition also shortlists the databases.
    const auto * other_columns_predicate = filter_by_other_columns ? filter_by_other_columns->getOutputs().at(0) : nullptr;
    const auto table_name_filter = extractNameFilter(other_columns_predicate, table_column_name, context);
    const auto database_name_filter = extractNameFilter(other_columns_predicate, database_column_name, context);

    MutableColumnPtr table_column_mut = ColumnString::create();
    MutableColumnPtr engine_column_mut = ColumnString::create();
    MutableColumnPtr active_column_mut = ColumnUInt8::create();
    MutableColumnPtr storage_uuid_column_mut = ColumnUUID::create();

    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    {
        Databases databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});

        /// Add column 'database'.
        MutableColumnPtr database_column_mut = ColumnString::create();
        for (const auto & database : databases)
        {
            /// Check if database can contain MergeTree tables,
            /// if not it's unnecessary to load all tables of database just to filter all of them.
            if (database.second->isExternal())
                continue;
            if (database_name_filter && !database_name_filter(database.first))
                continue;
            database_column_mut->insert(database.first);
        }
        block_to_filter.insert(ColumnWithTypeAndName(
            std::move(database_column_mut), std::make_shared<DataTypeString>(), database_column_name));

        /// Filter block_to_filter with column 'database'.
        if (filter_by_database)
            VirtualColumnUtils::filterBlockWithExpression(VirtualColumnUtils::buildFilterExpression(std::move(*filter_by_database), context), block_to_filter);
        rows = block_to_filter.rows();

        /// Block contains new columns, update database_column.
        ColumnPtr database_column_for_filter = block_to_filter.getByName(database_column_name).column;

        if (rows)
        {
            /// Add columns 'table', 'engine', 'active'

            IColumn::Offsets offsets(rows);

            /// Enumerating all tables of all databases can take a long time on a server with many tables,
            /// and this is done eagerly before returning the first row, so check for query cancellation
            /// and time limits here. If the time limit is exceeded in the 'break' mode,
            /// stop the enumeration instead of walking the remaining storages.
            bool time_limit_exceeded = false;

            for (size_t i = 0; i < rows; ++i)
            {
                offsets[i] = offsets[i - 1];

                if (time_limit_exceeded)
                    continue;

                String database_name = (*database_column_for_filter)[i].safeGet<String>();
                const DatabasePtr & database = databases.at(database_name);
                const bool check_access_for_tables_in_db
                    = check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name);

                offsets[i] = offsets[i - 1];
                auto iterator = database->getTablesIterator(context, table_name_filter, /* skip_not_loaded */ false);
                for (; iterator->isValid(); iterator->next())
                {
                    if (query_status && !query_status->checkTimeLimit())
                    {
                        time_limit_exceeded = true;
                        break;
                    }

                    String table_name = iterator->name();

                    slowDownSystemPartsDiscovery(table_name);

                    auto storage = castStorage<MergeTreeData>(iterator->table(), DeferredTable::Skip);
                    if (!storage)
                        continue;

                    String engine_name = storage->getName();
                    UUID storage_uuid = storage->getStorageID().uuid;
                    if (storage_uuid == UUIDHelpers::Nil)
                    {
                        SipHash hash;
                        hash.update(database_name);
                        hash.update(table_name);
                        storage_uuid = hash.get128();
                    }

                    if (check_access_for_tables_in_db && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                        continue;

                    storages[storage_uuid] = storage;

                    /// Add all combinations of flag 'active'.
                    for (UInt64 active : {0, 1})
                    {
                        table_column_mut->insert(table_name);
                        engine_column_mut->insert(engine_name);
                        active_column_mut->insert(active);
                        storage_uuid_column_mut->insert(storage_uuid);
                    }

                    offsets[i] += 2;
                }
            }

            for (size_t i = 0; i < block_to_filter.columns(); ++i)
            {
                ColumnPtr & column = block_to_filter.safeGetByPosition(i).column;
                column = column->replicate(offsets);
            }
        }
    }

    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), table_column_name));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(engine_column_mut), std::make_shared<DataTypeString>(), engine_column_name));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(active_column_mut), std::make_shared<DataTypeUInt8>(), active_column_name));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(storage_uuid_column_mut), std::make_shared<DataTypeUUID>(), storage_uuid_column_name));

    if (rows)
    {
        /// Filter block_to_filter with columns 'database', 'table', 'engine', 'active'.
        if (filter_by_other_columns)
            VirtualColumnUtils::filterBlockWithExpression(VirtualColumnUtils::buildFilterExpression(std::move(*filter_by_other_columns), context), block_to_filter);
        rows = block_to_filter.rows();
    }

    database_column = block_to_filter.getByName(database_column_name).column;
    table_column = block_to_filter.getByName(table_column_name).column;
    active_column = block_to_filter.getByName(active_column_name).column;
    storage_uuid_column = block_to_filter.getByName(storage_uuid_column_name).column;
}

/// Fills the rows part by part and returns a chunk once it reaches `max_block_size` rows or
/// `max_block_bytes` bytes: a row can carry a lot of part metadata, so a row limit alone does not bound memory.
class SystemPartsSource final : public ISource
{
public:
    SystemPartsSource(
        SharedHeader header_,
        std::shared_ptr<StorageSystemPartsBase> storage_,
        std::unique_ptr<StoragesInfoStreamBase> stream_,
        ContextPtr context_,
        std::vector<UInt8> columns_mask_,
        bool has_state_column_,
        size_t max_block_size_,
        size_t max_block_bytes_)
        : ISource(std::move(header_))
        , storage(std::move(storage_))
        , stream(std::move(stream_))
        , context(std::move(context_))
        , query_status(context->getProcessListElement())
        , columns_mask(std::move(columns_mask_))
        , has_state_column(has_state_column_)
        , max_block_size(max_block_size_)
        , max_block_bytes(max_block_bytes_)
    {
    }

    String getName() const override { return "SystemParts"; }

protected:
    Chunk generate() override;

private:
    /// Moves to the next storage that has parts. Returns false at the end of the stream.
    bool nextStorage();
    void releaseStorage();

    std::shared_ptr<StorageSystemPartsBase> storage;
    std::unique_ptr<StoragesInfoStreamBase> stream;
    ContextPtr context;
    QueryStatusPtr query_status;
    const std::vector<UInt8> columns_mask;
    const bool has_state_column;
    const size_t max_block_size;
    /// Zero means no limit.
    const size_t max_block_bytes;

    /// The storage whose parts are listed, with its table lock.
    StoragesInfo info;
    StorageSystemPartsBase::StorageProcessingStatePtr storage_state;
    MergeTreeData::DataPartsVector parts;
    MergeTreeData::DataPartStateVector parts_states;
    /// Keeps the parent parts of projection parts alive.
    MergeTreeData::DataPartsVector parent_parts;
    size_t next_part = 0;
    bool is_exhausted = false;
};

Chunk SystemPartsSource::generate()
{
    if (is_exhausted)
        return {};

    MutableColumns columns = getPort().getHeader().cloneEmptyColumns();

    while (true)
    {
        if (next_part == parts.size() && !nextStorage())
        {
            is_exhausted = true;
            break;
        }

        /// In the 'break' mode `checkTimeLimit` returns false instead of throwing:
        /// end the stream with the rows collected so far.
        if (query_status && !query_status->checkTimeLimit())
        {
            is_exhausted = true;
            break;
        }

        slowDownSystemPartsEnumeration(info.table);

        const size_t part_index = next_part++;
        storage->processPart(context, columns, columns_mask, info, parts[part_index], parts_states[part_index], has_state_column, *storage_state);

        const size_t num_rows = columns.at(0)->size();
        if (num_rows == 0)
            continue;

        if (num_rows >= max_block_size)
            break;

        if (max_block_bytes)
        {
            size_t num_bytes = 0;
            for (const auto & column : columns)
                num_bytes += column->byteSize();

            if (num_bytes >= max_block_bytes)
                break;
        }
    }

    if (is_exhausted)
        releaseStorage();

    const size_t num_rows = columns.at(0)->size();
    if (num_rows == 0)
        return {};

    return Chunk(std::move(columns), num_rows);
}

bool SystemPartsSource::nextStorage()
{
    while (true)
    {
        releaseStorage();

        info = stream->next();
        if (!info)
            return false;

        if (query_status && !query_status->checkTimeLimit())
            return false;

        storage_state = storage->prepareStorage(context, info, columns_mask);
        if (!storage_state)
            continue;

        if (storage->listsProjectionParts())
        {
            auto projection_parts = info.getProjectionParts(parts_states, has_state_column, query_status);
            parts = std::move(projection_parts.projection_parts);
            parent_parts = std::move(projection_parts.data_parts);
        }
        else
        {
            parts = info.getParts(parts_states, has_state_column, query_status);
        }

        if (!parts.empty())
            return true;
    }
}

void SystemPartsSource::releaseStorage()
{
    parts.clear();
    parts_states.clear();
    parent_parts.clear();
    next_part = 0;
    storage_state.reset();
    info = {};
}

class ReadFromSystemPartsBase : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemPartsBase"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemPartsBase(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        std::shared_ptr<StorageSystemPartsBase> storage_,
        std::vector<UInt8> columns_mask_,
        bool has_state_column_,
        size_t max_block_size_);

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

protected:
    std::shared_ptr<StorageSystemPartsBase> storage;
    std::vector<UInt8> columns_mask;
    const bool has_state_column;
    const size_t max_block_size;
    std::optional<ActionsDAG> filter_by_database;
    std::optional<ActionsDAG> filter_by_other_columns;
};

ReadFromSystemPartsBase::ReadFromSystemPartsBase(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    SharedHeader sample_block,
    std::shared_ptr<StorageSystemPartsBase> storage_,
    std::vector<UInt8> columns_mask_,
    bool has_state_column_,
    size_t max_block_size_)
    : SourceStepWithFilter(
        std::move(sample_block),
        column_names_,
        query_info_,
        storage_snapshot_,
        context_)
    , storage(std::move(storage_))
    , columns_mask(std::move(columns_mask_))
    , has_state_column(has_state_column_)
    , max_block_size(max_block_size_)
{
}

void ReadFromSystemPartsBase::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        const auto * predicate = filter_actions_dag->getOutputs().at(0);

        Block block;
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), database_column_name));

        filter_by_database = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &block, context);
        if (filter_by_database)
            VirtualColumnUtils::buildSetsForDAG(*filter_by_database, context);

        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), table_column_name));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), engine_column_name));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeUInt8>(), active_column_name));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeUUID>(), storage_uuid_column_name));

        filter_by_other_columns = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &block, context);
        /// `StoragesInfoStream` reads the condition on `table` back from this filter to narrow the
        /// enumeration, which needs the elements of an `IN` over a subquery: keep them.
        if (filter_by_other_columns)
            VirtualColumnUtils::buildSetsForDAGKeepingElements(*filter_by_other_columns, context);
    }
}

void StorageSystemPartsBase::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    bool has_state_column = hasStateColumn(column_names, storage_snapshot);

    /// Create the result.
    Block sample = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, header] = getQueriedColumnsMaskAndHeader(sample, column_names);

    if (has_state_column)
        header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "_state"));

    auto this_ptr = std::static_pointer_cast<StorageSystemPartsBase>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemPartsBase>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::make_shared<const Block>(std::move(header)), std::move(this_ptr), std::move(columns_mask), has_state_column,
        max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemPartsBase::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto stream = storage->getStoragesInfoStream(std::move(filter_by_database), std::move(filter_by_other_columns), context);

    pipeline.init(Pipe(std::make_shared<SystemPartsSource>(
        getOutputHeader(),
        storage,
        std::move(stream),
        context,
        columns_mask,
        has_state_column,
        max_block_size,
        context->getSettingsRef()[Setting::preferred_block_size_bytes])));
}


StorageSystemPartsBase::StorageSystemPartsBase(const StorageID & table_id_, ColumnsDescription && columns)
    : StorageWithCommonVirtualColumns(table_id_)
{
    auto add_alias = [&](const String & alias_name, const String & column_name)
    {
        if (!columns.has(column_name))
            return;
        ColumnDescription column(alias_name, columns.get(column_name).type);
        column.default_desc.kind = ColumnDefaultKind::Alias;
        column.default_desc.expression = make_intrusive<ASTIdentifier>(column_name);
        columns.add(column);
    };

    /// Add aliases for old column names for backwards compatibility.
    add_alias("bytes", "bytes_on_disk");
    add_alias("marks_size", "marks_bytes");
    add_alias("part_name", "name");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemPartsBase::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_state", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

bool StoragesInfoStreamBase::tryLockTable(StoragesInfo & info)
{
    const Poco::Timespan acquire_timeout(lock_timeout.count() * 1000);

    if (!query_status)
    {
        info.table_lock = info.storage->tryLockForShare(query_id, acquire_timeout);
        // nullptr means table was dropped while acquiring the lock
        return info.table_lock != nullptr;
    }

    /// Wait for the lock in short slices, polling the query status between them, so that a killed
    /// or soft-timed-out query does not sit inside RWLockImpl::getLock for the whole
    /// lock_acquire_timeout while a concurrent DDL query holds the drop lock. The slicing is done
    /// by IStorage below its throwing API boundary: an expired slice is a plain retry, not an
    /// exception, and only the exhaustion of the whole lock_acquire_timeout throws DEADLOCK_AVOIDED.
    static const Poco::Timespan cancellation_check_period(100 * 1000);

    /// checkTimeLimit throws if the query is cancelled or the time limit is exceeded in the 'throw'
    /// overflow mode. In the 'break' mode it returns false instead: give up on this table, and the
    /// caller stops instead of failing the query.
    auto need_stop = [this] { return !query_status->checkTimeLimit(); };

    info.table_lock = info.storage->tryLockForShare(query_id, acquire_timeout, need_stop, cancellation_check_period);
    // nullptr means the table was dropped while acquiring the lock, or the query was stopped
    return info.table_lock != nullptr;
}
}
