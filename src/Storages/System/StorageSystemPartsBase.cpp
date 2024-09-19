#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Common/escapeForFileName.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMaterializedMySQL.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

namespace
{
constexpr auto * database_column_name = "database";
constexpr auto * table_column_name = "table";
constexpr auto * engine_column_name = "engine";
constexpr auto * active_column_name = "active";
constexpr auto * storage_uuid_column_name = "storage_uuid";
}

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

StoragesInfoStreamBase::StoragesInfoStreamBase(ContextPtr context)
    : query_id(context->getCurrentQueryId()), lock_timeout(context->getSettingsRef()[Setting::lock_acquire_timeout]), next_row(0), rows(0)
{
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

MergeTreeData::DataPartsVector
StoragesInfo::getParts(MergeTreeData::DataPartStateVector & state, bool has_state_column) const
{
    using State = MergeTreeData::DataPartState;
    if (need_inactive_parts)
    {
        /// If has_state_column is requested, return all states.
        if (!has_state_column)
            return data->getDataPartsVectorForInternalUsage({State::Active, State::Outdated}, &state);

        return data->getAllDataPartsVector(&state);
    }

    return data->getDataPartsVectorForInternalUsage({State::Active}, &state);
}

MergeTreeData::ProjectionPartsVector
StoragesInfo::getProjectionParts(MergeTreeData::DataPartStateVector & state, bool has_state_column) const
{
    if (data->getInMemoryMetadataPtr()->projections.empty())
        return {};

    using State = MergeTreeData::DataPartState;
    if (need_inactive_parts)
    {
        /// If has_state_column is requested, return all states.
        if (!has_state_column)
            return data->getProjectionPartsVectorForInternalUsage({State::Active, State::Outdated}, &state);

        return data->getAllProjectionPartsVector(&state);
    }

    return data->getProjectionPartsVectorForInternalUsage({State::Active}, &state);
}

StoragesInfoStream::StoragesInfoStream(std::optional<ActionsDAG> filter_by_database, std::optional<ActionsDAG> filter_by_other_columns, ContextPtr context)
    : StoragesInfoStreamBase(context)
{
    /// Will apply WHERE to subset of columns and then add more columns.
    /// This is kind of complicated, but we use WHERE to do less work.

    Block block_to_filter;

    MutableColumnPtr table_column_mut = ColumnString::create();
    MutableColumnPtr engine_column_mut = ColumnString::create();
    MutableColumnPtr active_column_mut = ColumnUInt8::create();
    MutableColumnPtr storage_uuid_column_mut = ColumnUUID::create();

    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    {
        Databases databases = DatabaseCatalog::instance().getDatabases();

        /// Add column 'database'.
        MutableColumnPtr database_column_mut = ColumnString::create();
        for (const auto & database : databases)
        {
            /// Check if database can contain MergeTree tables,
            /// if not it's unnecessary to load all tables of database just to filter all of them.
            if (database.second->canContainMergeTreeTables())
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

            for (size_t i = 0; i < rows; ++i)
            {
                String database_name = (*database_column_for_filter)[i].safeGet<String>();
                const DatabasePtr database = databases.at(database_name);

                offsets[i] = i ? offsets[i - 1] : 0;
                for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
                {
                    String table_name = iterator->name();
                    StoragePtr storage = iterator->table();
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

#if USE_MYSQL
                    if (auto * proxy = dynamic_cast<StorageMaterializedMySQL *>(storage.get()))
                    {
                        auto nested = proxy->getNested();
                        storage.swap(nested);
                    }
#endif
                    if (!dynamic_cast<MergeTreeData *>(storage.get()))
                        continue;

                    if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
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
        Block sample_block,
        std::shared_ptr<StorageSystemPartsBase> storage_,
        std::vector<UInt8> columns_mask_,
        bool has_state_column_);

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

protected:
    std::shared_ptr<StorageSystemPartsBase> storage;
    std::vector<UInt8> columns_mask;
    const bool has_state_column;
    std::optional<ActionsDAG> filter_by_database;
    std::optional<ActionsDAG> filter_by_other_columns;
};

ReadFromSystemPartsBase::ReadFromSystemPartsBase(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    Block sample_block,
    std::shared_ptr<StorageSystemPartsBase> storage_,
    std::vector<UInt8> columns_mask_,
    bool has_state_column_)
    : SourceStepWithFilter(
        DataStream{.header = std::move(sample_block)},
        column_names_,
        query_info_,
        storage_snapshot_,
        context_)
    , storage(std::move(storage_))
    , columns_mask(std::move(columns_mask_))
    , has_state_column(has_state_column_)
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

        filter_by_database = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &block);
        if (filter_by_database)
            VirtualColumnUtils::buildSetsForDAG(*filter_by_database, context);

        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), table_column_name));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeString>(), engine_column_name));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeUInt8>(), active_column_name));
        block.insert(ColumnWithTypeAndName({}, std::make_shared<DataTypeUUID>(), storage_uuid_column_name));

        filter_by_other_columns = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &block);
        if (filter_by_other_columns)
            VirtualColumnUtils::buildSetsForDAG(*filter_by_other_columns, context);
    }
}

void StorageSystemPartsBase::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
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
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), has_state_column);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemPartsBase::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto stream = storage->getStoragesInfoStream(std::move(filter_by_database), std::move(filter_by_other_columns), context);
    auto header = getOutputStream().header;

    MutableColumns res_columns = header.cloneEmptyColumns();

    while (StoragesInfo info = stream->next())
    {
        storage->processNextStorage(context, res_columns, columns_mask, info, has_state_column);
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    pipeline.init(Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk))));
}


StorageSystemPartsBase::StorageSystemPartsBase(const StorageID & table_id_, ColumnsDescription && columns)
    : IStorage(table_id_)
{
    auto add_alias = [&](const String & alias_name, const String & column_name)
    {
        if (!columns.has(column_name))
            return;
        ColumnDescription column(alias_name, columns.get(column_name).type);
        column.default_desc.kind = ColumnDefaultKind::Alias;
        column.default_desc.expression = std::make_shared<ASTIdentifier>(column_name);
        columns.add(column);
    };

    /// Add aliases for old column names for backwards compatibility.
    add_alias("bytes", "bytes_on_disk");
    add_alias("marks_size", "marks_bytes");
    add_alias("part_name", "name");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);

    VirtualColumnsDescription virtuals;
    virtuals.addEphemeral("_state", std::make_shared<DataTypeString>(), "");
    setVirtuals(std::move(virtuals));
}

bool StoragesInfoStreamBase::tryLockTable(StoragesInfo & info)
{
    info.table_lock = info.storage->tryLockForShare(query_id, lock_timeout);
    // nullptr means table was dropped while acquiring the lock
    return info.table_lock != nullptr;
}
}
