#include <Storages/System/StorageSystemTables.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageView.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/typeid_cast.h>

#include <boost/range/adaptor/map.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 select_sequential_consistency;
    extern const SettingsBool show_table_uuid_in_table_create_query_if_not_nil;
    extern const SettingsBool show_data_lake_catalogs_in_system_tables;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace detail
{
ColumnPtr getFilteredDatabases(const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr column = ColumnString::create();

    const auto & settings = context->getSettingsRef();
    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = settings[Setting::show_data_lake_catalogs_in_system_tables]});
    for (const auto & database_name : databases | boost::adaptors::map_keys)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.tables

        column->insert(database_name);
    }

    Block block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database")};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

ColumnPtr getFilteredTables(
    const ActionsDAG::Node * predicate, const ColumnPtr & filtered_databases_column, ContextPtr context, const bool is_detached)
{
    Block sample{
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "uuid"),
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "engine")};

    MutableColumnPtr table_column = ColumnString::create();
    MutableColumnPtr uuid_column;
    MutableColumnPtr engine_column;

    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &sample, context);
    if (dag)
    {
        bool filter_by_engine = false;
        bool filter_by_uuid = false;
        for (const auto * input : dag->getInputs())
        {
            if (input->result_name == "engine")
                filter_by_engine = true;

            if (input->result_name == "uuid")
                filter_by_uuid = true;
        }

        if (filter_by_engine)
            engine_column = ColumnString::create();

        if (filter_by_uuid)
            uuid_column = ColumnUUID::create();
    }

    for (size_t database_idx = 0; database_idx < filtered_databases_column->size(); ++database_idx)
    {
        const auto database_name = filtered_databases_column->getDataAt(database_idx);
        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
            continue;

        if (is_detached)
        {
            auto table_it = database->getDetachedTablesIterator(context, {}, false);
            for (; table_it->isValid(); table_it->next())
            {
                table_column->insert(table_it->table());
            }
        }
        else
        {
            if (engine_column || uuid_column)
            {
                auto table_it = database->getTablesIterator(context,
                                                                       /* filter_by_table_name */ {},
                                                                       /* skip_not_loaded */ false);
                for (; table_it->isValid(); table_it->next())
                {
                    table_column->insert(table_it->name());
                    if (engine_column)
                        engine_column->insert(table_it->table()->getName());
                    if (uuid_column)
                        uuid_column->insert(table_it->table()->getStorageID().uuid);
                }
            }
            else
            {
                auto table_details = database->getLightweightTablesIterator(context,
                                                                      /* filter_by_table_name */ {},
                                                                      /* skip_not_loaded */ false);
                for (const auto & table_detail : table_details)
                {
                    table_column->insert(table_detail.name);
                }
            }
        }
    }

    Block block{ColumnWithTypeAndName(std::move(table_column), std::make_shared<DataTypeString>(), "name")};
    if (engine_column)
        block.insert(ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine"));
    if (uuid_column)
        block.insert(ColumnWithTypeAndName(std::move(uuid_column), std::make_shared<DataTypeUUID>(), "uuid"));

    if (dag)
        VirtualColumnUtils::filterBlockWithExpression(VirtualColumnUtils::buildFilterExpression(std::move(*dag), context), block);

    return block.getByPosition(0).column;
}

}

StorageSystemTables::StorageSystemTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    auto description = ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        {"name", std::make_shared<DataTypeString>(), "Table name."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Table uuid (Atomic database)."},
        {"engine", std::make_shared<DataTypeString>(), "Table engine name (without parameters)."},
        {"is_temporary", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the table is temporary."},
        {"data_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Paths to the table data in the file systems."},
        {"metadata_path", std::make_shared<DataTypeString>(), "Path to the table metadata in the file system."},
        {"metadata_modification_time", std::make_shared<DataTypeDateTime>(), "Time of latest modification of the table metadata."},
        {"metadata_version", std::make_shared<DataTypeInt32>(), "Metadata version for ReplicatedMergeTree table, 0 for non ReplicatedMergeTree table."},
        {"dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Database dependencies."},
        {"dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Table dependencies (materialized views the current table)."},
        {"create_table_query", std::make_shared<DataTypeString>(), "The query that was used to create the table."},
        {"engine_full", std::make_shared<DataTypeString>(), "Parameters of the table engine."},
        {"as_select", std::make_shared<DataTypeString>(), "SELECT query for view."},
        {"parameterized_view_parameters",
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}, Names{"name", "type"})),
            "Parameters of parameterized view."
        },
        {"partition_key", std::make_shared<DataTypeString>(), "The partition key expression specified in the table."},
        {"sorting_key", std::make_shared<DataTypeString>(), "The sorting key expression specified in the table."},
        {"primary_key", std::make_shared<DataTypeString>(), "The primary key expression specified in the table."},
        {"sampling_key", std::make_shared<DataTypeString>(), "The sampling key expression specified in the table."},
        {"storage_policy", std::make_shared<DataTypeString>(), "The storage policy."},
        {"total_rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Total number of rows, if it is possible to quickly determine exact number of rows in the table, otherwise NULL (including underlying Buffer table)."
        },
        {"total_bytes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Total number of bytes, if it is possible to quickly determine exact number "
            "of bytes for the table on storage, otherwise NULL (does not includes any underlying storage). "
            "If the table stores data on disk, returns used space on disk (i.e. compressed). "
            "If the table stores data in memory, returns approximated number of used bytes in memory."
        },
        {"total_bytes_uncompressed", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Total number of uncompressed bytes, if it's possible to quickly determine the exact number "
            "of bytes from the part checksums for the table on storage, otherwise NULL (does not take underlying storage (if any) into account)."
        },
        {"parts", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The total number of parts in this table."},
        {"active_parts", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The number of active parts in this table."},
        {"total_marks", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The total number of marks in all parts in this table."},
        {"active_on_fly_data_mutations", std::make_shared<DataTypeUInt64>(), "Total number of active data mutations (UPDATEs and DELETEs) suitable for applying on the fly."},
        {"active_on_fly_alter_mutations", std::make_shared<DataTypeUInt64>(), "Total number of active alter mutations (MODIFY COLUMNs) suitable for applying on the fly."},
        {"active_on_fly_metadata_mutations", std::make_shared<DataTypeUInt64>(), "Total number of active metadata mutations (RENAMEs) suitable for applying on the fly."},
        {"columns_descriptions_cache_size", std::make_shared<DataTypeUInt64>(), "Size of columns description cache for *MergeTree tables"},
        {"lifetime_rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Total number of rows INSERTed since server start (only for Buffer tables)."
        },
        {"lifetime_bytes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Total number of bytes INSERTed since server start (only for Buffer tables)."
        },
        {"comment", std::make_shared<DataTypeString>(), "The comment for the table."},
        {"has_own_data", std::make_shared<DataTypeUInt8>(),
            "Flag that indicates whether the table itself stores some data on disk or only accesses some other source."
        },
        {"loading_dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Database loading dependencies (list of objects which should be loaded before the current object)."
        },
        {"loading_dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Table loading dependencies (list of objects which should be loaded before the current object)."
        },
        {"loading_dependent_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Dependent loading database."
        },
        {"loading_dependent_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Dependent loading table."
        },
        {"definer", std::make_shared<DataTypeString>(), "SQL security definer's name used for the table."},
    };

    description.setAliases({
        {"table", std::make_shared<DataTypeString>(), "name"}
    });

    storage_metadata.setColumns(std::move(description));
    setInMemoryMetadata(storage_metadata);
}

class TablesBlockSource : public ISource
{
public:
    TablesBlockSource(
        std::vector<UInt8> columns_mask_,
        SharedHeader header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr tables_,
        ContextPtr context_)
        : ISource(std::move(header))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
    {
        size_t size = tables_->size();
        tables.reserve(size);
        for (size_t idx = 0; idx < size; ++idx)
            tables.insert(std::string{tables_->getDataAt(idx)});
    }

    String getName() const override { return "Tables"; }

protected:
    NameToNameMap getSelectParamters(const StorageMetadataPtr & metadata_snapshot)
    {
        const SelectQueryDescription & query_description = metadata_snapshot->getSelectQuery();
        ASTPtr inner_query = query_description.inner_query;
        if (!inner_query || !inner_query->as<ASTSelectWithUnionQuery>())
            return {};

        return inner_query->as<ASTSelectWithUnionQuery>()->getQueryParameters();
    }

    void fillParametralizedViewData(MutableColumns & columns, const StoragePtr & table, size_t & res_index)
    {
        if (table)
        {
            StorageMetadataPtr metadata_snapshot = table->tryGetInMemoryMetadataPtr().value_or(nullptr);
            if (!metadata_snapshot)
            {
                columns[res_index++]->insertDefault();
                return;
            }

            NameToNameMap query_parameters_array = getSelectParamters(metadata_snapshot);
            if (!query_parameters_array.empty())
            {
                Array changes;
                for (const auto & [key, value] : query_parameters_array)
                    changes.push_back(Tuple{key, value});
                columns[res_index++]->insert(changes);
            }
            else
                columns[res_index++]->insertDefault();
        }
    }


    size_t fillTableNamesOnly(MutableColumns & res_columns)
    {
        auto table_details = database->getLightweightTablesIterator(context,
                                /* filter_by_table_name */ {},
                                /* skip_not_loaded */ false);

        size_t count = 0;

        const auto access = context->getAccess();
        for (const auto & table_detail: table_details)
        {
            if (!tables.contains(table_detail.name))
                continue;

            size_t src_index = 0;
            size_t res_index = 0;

            if (!access->isGranted(AccessType::SHOW_TABLES, database_name, table_detail.name))
                continue;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(database_name);

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(table_detail.name);

            ++count;
        }
        ++database_idx;
        return count;
    }

    Chunk generate() override
    {
        if (done)
            return {};

        auto component_guard = Coordination::setCurrentComponent("TablesBlockSource::generate");

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx);
                database = DatabaseCatalog::instance().tryGetDatabase(database_name);

                if (!database)
                {
                    /// Database was deleted just now or the user has no access.
                    ++database_idx;
                    continue;
                }

                break;
            }

            /// This is for temporary tables. They are output in single block regardless to max_block_size.
            if (database_idx >= databases->size())
            {
                if (context->hasSessionContext())
                {
                    Tables external_tables = context->getSessionContext()->getExternalTables();

                    for (auto & table : external_tables)
                    {
                        size_t src_index = 0;
                        size_t res_index = 0;

                        // database
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // name
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.first);

                        // uuid
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getStorageID().uuid);

                        // engine
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        // is_temporary
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(1u);

                        // data_paths
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // metadata_path
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // metadata_modification_time
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // metadata_version
                        // Temporary tables does not support replication
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // dependencies_database
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // dependencies_table
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // create_table_query
                        if (columns_mask[src_index++])
                        {
                            auto temp_db = DatabaseCatalog::instance().getDatabaseForTemporaryTables();
                            ASTPtr ast = temp_db ? temp_db->tryGetCreateTableQuery(table.second->getStorageID().getTableName(), context) : nullptr;
                            res_columns[res_index++]->insert(ast ? format({context, *ast}) : "");
                        }

                        // engine_full
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        while (src_index < columns_mask.size())
                        {
                            // total_rows
                            if (src_index == 14 && columns_mask[src_index])
                            {
                                // parameterized view parameters
                                fillParametralizedViewData(res_columns, table.second, res_index);
                            }
                            else if (src_index == 20 && columns_mask[src_index])
                            {
                                try
                                {
                                    if (auto total_rows = table.second->totalRows(context))
                                        res_columns[res_index]->insert(*total_rows);
                                    else
                                        res_columns[res_index]->insertDefault();
                                }
                                catch (const Exception &)
                                {
                                    /// Even if the method throws, it should not prevent querying system.tables.
                                    tryLogCurrentException("StorageSystemTables");
                                    res_columns[res_index]->insertDefault();
                                }
                                ++res_index;
                            }
                            // total_bytes
                            else if (src_index == 21 && columns_mask[src_index])
                            {
                                try
                                {
                                    if (auto total_bytes = table.second->totalBytes(context))
                                        res_columns[res_index]->insert(*total_bytes);
                                    else
                                        res_columns[res_index]->insertDefault();
                                }
                                catch (const Exception &)
                                {
                                    /// Even if the method throws, it should not prevent querying system.tables.
                                    tryLogCurrentException("StorageSystemTables");
                                    res_columns[res_index]->insertDefault();
                                }
                                ++res_index;
                            }
                            /// Fill the rest columns with defaults
                            else if (columns_mask[src_index])
                                res_columns[res_index++]->insertDefault();
                            src_index++;
                        }
                    }
                }

                UInt64 num_rows = res_columns.at(0)->size();
                done = true;
                return Chunk(std::move(res_columns), num_rows);
            }

            const bool need_to_check_access_for_tables = need_to_check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            /// This is for queries similar to 'show tables', where only name of the table is needed
            auto needed_columns = getPort().getHeader().getColumnsWithTypeAndName();
            bool needs_one_column = (needed_columns.size() == 1 && needed_columns[0].name == "name");

            bool needs_two_columns = (needed_columns.size() == 2 &&
                        ((needed_columns[0].name == "name" && needed_columns[1].name == "database") ||
                            (needed_columns[0].name == "database" && needed_columns[1].name == "name")));

            if ((needs_one_column || needs_two_columns) && !need_to_check_access_for_tables)
            {
                size_t rows_added = fillTableNamesOnly(res_columns);
                rows_count += rows_added;
                continue;
            }

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesIterator(context,
                        /* filter_by_table_name */ {},
                        /* skip_not_loaded */ false);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (!tables.contains(table_name))
                    continue;

                if (need_to_check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                StoragePtr table = tables_it->table();
                if (!table)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database iterator returned nullptr for the table, which is a bug");

                TableLockHolder lock;

                /// The only column that requires us to hold a shared lock is data_paths as rename might alter them (on ordinary tables)
                /// and it's not protected internally by other mutexes
                static const size_t DATA_PATHS_INDEX = 5;
                if (columns_mask[DATA_PATHS_INDEX])
                {
                    lock = table->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
                    if (!lock)
                        // Table was dropped while acquiring the lock, skipping table
                        continue;
                }

                ++rows_count;

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(tables_it->uuid());

                if (columns_mask[src_index++])
                {
                    chassert(table != nullptr);
                    res_columns[res_index++]->insert(table->getName());
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(0u);  // is_temporary

                if (columns_mask[src_index++])
                {
                    chassert(lock != nullptr);
                    Array table_paths_array;
                    if (auto paths = table->tryGetDataPaths())
                    {
                        table_paths_array.reserve(paths->size());
                        for (const String & path : *paths)
                            table_paths_array.push_back(path);
                    }
                    res_columns[res_index++]->insert(table_paths_array);
                    /// We don't need the lock anymore
                    lock = nullptr;
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database->getObjectMetadataPath(table_name));

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(static_cast<UInt64>(database->getObjectMetadataModificationTime(table_name)));

                StorageMetadataPtr metadata_snapshot;
                if (table)
                    metadata_snapshot = table->tryGetInMemoryMetadataPtr().value_or(nullptr);

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && table->supportsReplication())
                        res_columns[res_index++]->insert(metadata_snapshot->metadata_version);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                {
                    Array views_table_name_array;
                    Array views_database_name_array;
                    if (columns_mask[src_index] || columns_mask[src_index + 1])
                    {
                        const auto view_ids = DatabaseCatalog::instance().getDependentViews(StorageID(database_name, table_name));

                        views_table_name_array.reserve(view_ids.size());
                        views_database_name_array.reserve(view_ids.size());
                        for (const auto & view_id : view_ids)
                        {
                            views_table_name_array.push_back(view_id.table_name);
                            views_database_name_array.push_back(view_id.database_name);
                        }
                    }

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(views_database_name_array);

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(views_table_name_array);
                }

                if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2])
                {
                    ASTPtr ast = database->tryGetCreateTableQuery(table_name, context);
                    auto * ast_create = ast ? ast->as<ASTCreateQuery>() : nullptr;

                    if (ast_create && !context->getSettingsRef()[Setting::show_table_uuid_in_table_create_query_if_not_nil])
                    {
                        ast_create->uuid = UUIDHelpers::Nil;
                        if (ast_create->targets)
                            ast_create->targets->resetInnerUUIDs();
                    }

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(ast ? format({context, *ast}) : "");

                    if (columns_mask[src_index++])
                    {
                        String engine_full;

                        if (ast_create && ast_create->storage)
                        {
                            engine_full = format({context, *ast_create->storage});

                            static const char * const extra_head = " ENGINE = ";
                            if (startsWith(engine_full, extra_head))
                                engine_full = engine_full.substr(strlen(extra_head));
                        }

                        res_columns[res_index++]->insert(engine_full);
                    }

                    if (columns_mask[src_index++])
                    {
                        String as_select;
                        if (ast_create && ast_create->select)
                            as_select = format({context, *ast_create->select});
                        res_columns[res_index++]->insert(as_select);
                    }
                }
                else
                    src_index += 3;

                // parameterized view parameters
                if (columns_mask[src_index++])
                    fillParametralizedViewData(res_columns, table, res_index);

                ASTPtr expression_ptr;
                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getPartitionKeyAST()))
                        res_columns[res_index++]->insert(format({context, *expression_ptr}));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getSortingKey().expression_list_ast))
                        res_columns[res_index++]->insert(format({context, *expression_ptr}));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getPrimaryKey().expression_list_ast))
                        res_columns[res_index++]->insert(format({context, *expression_ptr}));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && (expression_ptr = metadata_snapshot->getSamplingKeyAST()))
                        res_columns[res_index++]->insert(format({context, *expression_ptr}));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto policy = table ? table->tryGetStoragePolicy().value_or(nullptr) : nullptr;
                    if (policy)
                        res_columns[res_index++]->insert(policy->getName());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                ContextMutablePtr context_copy = Context::createCopy(context);
                Settings settings_copy = context_copy->getSettingsCopy();
                settings_copy[Setting::select_sequential_consistency] = 0;
                context_copy->setSettings(settings_copy);

                if (columns_mask[src_index++])
                {
                    try
                    {
                        auto total_rows = table ? table->totalRows(context) : std::nullopt;
                        if (total_rows)
                            res_columns[res_index]->insert(*total_rows);
                        else
                            res_columns[res_index]->insertDefault();
                    }
                    catch (const Exception &)
                    {
                        /// Even if the method throws, it should not prevent querying system.tables.
                        tryLogCurrentException("StorageSystemTables");
                        res_columns[res_index]->insertDefault();
                    }
                    ++res_index;
                }

                if (columns_mask[src_index++])
                {
                    try
                    {
                        auto total_bytes = table->totalBytes(context_copy);
                        if (total_bytes)
                            res_columns[res_index]->insert(*total_bytes);
                        else
                            res_columns[res_index]->insertDefault();
                    }
                    catch (const Exception &)
                    {
                        /// Even if the method throws, it should not prevent querying system.tables.
                        tryLogCurrentException("StorageSystemTables");
                        res_columns[res_index]->insertDefault();
                    }
                    ++res_index;
                }

                if (columns_mask[src_index++])
                {
                    try
                    {
                        auto total_bytes_uncompressed = table->totalBytesUncompressed(context_copy->getSettingsRef());
                        if (total_bytes_uncompressed)
                            res_columns[res_index]->insert(*total_bytes_uncompressed);
                        else
                            res_columns[res_index]->insertDefault();
                    }
                    catch (const Exception &)
                    {
                        /// Even if the method throws, it should not prevent querying system.tables.
                        tryLogCurrentException("StorageSystemTables");
                        res_columns[res_index]->insertDefault();
                    }
                    ++res_index;
                }

                auto table_merge_tree = std::dynamic_pointer_cast<MergeTreeData>(table);
                if (columns_mask[src_index++])
                {
                    if (table_merge_tree)
                        res_columns[res_index++]->insert(table_merge_tree->getAllPartsCount());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (table_merge_tree)
                        res_columns[res_index++]->insert(table_merge_tree->getActivePartsCount());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (table_merge_tree)
                        res_columns[res_index++]->insert(table_merge_tree->getTotalMarksCount());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (table_merge_tree)
                {
                    MutationCounters mutation_counters;
                    if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2])
                        mutation_counters = table_merge_tree->getMutationCounters();

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(mutation_counters.num_data);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(mutation_counters.num_alter);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(mutation_counters.num_metadata);
                }
                else
                {
                    for (size_t i = 0; i < 3; ++i)
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                    }
                }

                // columns_descriptions_cache_size
                if (columns_mask[src_index++])
                {
                    if (table_merge_tree)
                        res_columns[res_index++]->insert(table_merge_tree->getColumnsDescriptionsCacheSize());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto lifetime_rows = table ? table->tryLifetimeRows().value_or(std::nullopt) : std::nullopt;
                    if (lifetime_rows)
                        res_columns[res_index++]->insert(*lifetime_rows);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    auto lifetime_bytes = table ? table->tryLifetimeBytes().value_or(std::nullopt) : std::nullopt;
                    if (lifetime_bytes)
                        res_columns[res_index++]->insert(*lifetime_bytes);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot)
                        res_columns[res_index++]->insert(metadata_snapshot->comment);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (table)
                        res_columns[res_index++]->insert(table->storesDataOnDisk());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index] || columns_mask[src_index + 1] || columns_mask[src_index + 2] || columns_mask[src_index + 3])
                {
                    auto dependencies = DatabaseCatalog::instance().getLoadingDependencies(StorageID{database_name, table_name});
                    auto dependents = DatabaseCatalog::instance().getLoadingDependents(StorageID{database_name, table_name});

                    Array dependencies_databases;
                    Array dependencies_tables;
                    dependencies_databases.reserve(dependencies.size());
                    dependencies_tables.reserve(dependencies.size());
                    for (const auto & dependency : dependencies)
                    {
                        dependencies_databases.push_back(dependency.database_name);
                        dependencies_tables.push_back(dependency.table_name);
                    }

                    Array dependents_databases;
                    Array dependents_tables;
                    dependents_databases.reserve(dependents.size());
                    dependents_tables.reserve(dependents.size());
                    for (const auto & dependent : dependents)
                    {
                        dependents_databases.push_back(dependent.database_name);
                        dependents_tables.push_back(dependent.table_name);
                    }

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependencies_databases);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependencies_tables);

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependents_databases);
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependents_tables);
                }
                else
                {
                    src_index += 4;
                }

                if (columns_mask[src_index++])
                {
                    if (metadata_snapshot && metadata_snapshot->sql_security_type == SQLSecurityType::DEFINER)
                        res_columns[res_index++]->insert(*metadata_snapshot->definer);
                    else
                        res_columns[res_index++]->insertDefault();
                }
            }
        }
        UInt64 num_rows = res_columns.at(0)->size();
        return Chunk(std::move(res_columns), num_rows);
    }
private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    NameSet tables;
    size_t database_idx = 0;
    DatabaseTablesIteratorPtr tables_it;
    ContextPtr context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;
};

class ReadFromSystemTables : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemTables"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemTables(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(std::move(sample_block)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::vector<UInt8> columns_mask;
    size_t max_block_size;

    ColumnPtr filtered_databases_column;
    ColumnPtr filtered_tables_column;
};

void StorageSystemTables::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, res_block] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto reading = std::make_unique<ReadFromSystemTables>(
        column_names, query_info, storage_snapshot, context, std::move(res_block), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemTables::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    filtered_databases_column = detail::getFilteredDatabases(predicate, context);
    filtered_tables_column = detail::getFilteredTables(predicate, filtered_databases_column, context, false);
}

void ReadFromSystemTables::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipe pipe(std::make_shared<TablesBlockSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases_column), std::move(filtered_tables_column), context));
    pipeline.init(std::move(pipe));
}

}
