#include <Common/SipHash.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Common/escapeForFileName.h>
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
#include <Parsers/queryToString.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>


namespace DB
{

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

StoragesInfoStream::StoragesInfoStream(const SelectQueryInfo & query_info, ContextPtr context)
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
            std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));

        /// Filter block_to_filter with column 'database'.
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);
        rows = block_to_filter.rows();

        /// Block contains new columns, update database_column.
        ColumnPtr database_column_for_filter = block_to_filter.getByName("database").column;

        if (rows)
        {
            /// Add columns 'table', 'engine', 'active'

            IColumn::Offsets offsets(rows);

            for (size_t i = 0; i < rows; ++i)
            {
                String database_name = (*database_column_for_filter)[i].get<String>();
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
                    if (database->getEngineName() == "Ordinary")
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

    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(engine_column_mut), std::make_shared<DataTypeString>(), "engine"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(active_column_mut), std::make_shared<DataTypeUInt8>(), "active"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(storage_uuid_column_mut), std::make_shared<DataTypeUUID>(), "uuid"));

    if (rows)
    {
        /// Filter block_to_filter with columns 'database', 'table', 'engine', 'active'.
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);
        rows = block_to_filter.rows();
    }

    database_column = block_to_filter.getByName("database").column;
    table_column = block_to_filter.getByName("table").column;
    active_column = block_to_filter.getByName("active").column;
    storage_uuid_column = block_to_filter.getByName("uuid").column;
}


Pipe StorageSystemPartsBase::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    bool has_state_column = hasStateColumn(column_names, storage_snapshot);

    auto stream = getStoragesInfoStream(query_info, context);

    /// Create the result.
    Block sample = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, header] = getQueriedColumnsMaskAndHeader(sample, column_names);

    MutableColumns res_columns = header.cloneEmptyColumns();
    if (has_state_column)
        res_columns.push_back(ColumnString::create());

    while (StoragesInfo info = stream->next())
    {
        processNextStorage(context, res_columns, columns_mask, info, has_state_column);
    }

    if (has_state_column)
        header.insert(ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "_state"));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}


StorageSystemPartsBase::StorageSystemPartsBase(const StorageID & table_id_, NamesAndTypesList && columns_)
    : IStorage(table_id_)
{
    ColumnsDescription tmp_columns(std::move(columns_));

    auto add_alias = [&](const String & alias_name, const String & column_name)
    {
        if (!tmp_columns.has(column_name))
            return;
        ColumnDescription column(alias_name, tmp_columns.get(column_name).type);
        column.default_desc.kind = ColumnDefaultKind::Alias;
        column.default_desc.expression = std::make_shared<ASTIdentifier>(column_name);
        tmp_columns.add(column);
    };

    /// Add aliases for old column names for backwards compatibility.
    add_alias("bytes", "bytes_on_disk");
    add_alias("marks_size", "marks_bytes");
    add_alias("part_name", "name");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(tmp_columns);
    setInMemoryMetadata(storage_metadata);
}

NamesAndTypesList StorageSystemPartsBase::getVirtuals() const
{
    return NamesAndTypesList{
        NameAndTypePair("_state", std::make_shared<DataTypeString>())
    };
}
}
