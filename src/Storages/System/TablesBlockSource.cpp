#include "TablesBlockSource.h"

#include <memory>

#include <Access/ContextAccess.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

namespace
{

/// Avoid heavy operation on tables if we only queried columns that we can get without table object.
/// Otherwise it will require table initialization for Lazy database.
bool needTable(const DatabasePtr & database, const Block & header)
{
    if (database->getEngineName() != "Lazy")
        return true;

    static const std::set<std::string> columns_without_table = {"database", "name", "uuid", "metadata_modification_time"};
    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        if (columns_without_table.find(column.name) == columns_without_table.end())
            return true;
    }
    return false;
}

}

Chunk TablesBlockSource::generate()
{
    if (done)
        return {};

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
            database_name = databases->getDataAt(database_idx).toString();
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
                        ASTPtr ast
                            = temp_db ? temp_db->tryGetCreateTableQuery(table.second->getStorageID().getTableName(), context) : nullptr;
                        res_columns[res_index++]->insert(ast ? format({context, *ast}) : "");
                    }

                    // engine_full
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(table.second->getName());

                    const auto & settings = context->getSettingsRef();
                    while (src_index < columns_mask.size())
                    {
                        // total_rows
                        if (src_index == 19 && columns_mask[src_index])
                        {
                            if (auto total_rows = table.second->totalRows(settings))
                                res_columns[res_index++]->insert(*total_rows);
                            else
                                res_columns[res_index++]->insertDefault();
                        }
                        // total_bytes
                        else if (src_index == 20 && columns_mask[src_index])
                        {
                            if (auto total_bytes = table.second->totalBytes(settings))
                                res_columns[res_index++]->insert(*total_bytes);
                            else
                                res_columns[res_index++]->insertDefault();
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

        const bool need_to_check_access_for_tables
            = need_to_check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

        if (!tables_it || !tables_it->isValid())
            tables_it = database->getTablesIterator(context);

        const bool need_table = needTable(database, getPort().getHeader());

        for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
        {
            auto table_name = tables_it->name();
            if (!tables.contains(table_name))
                continue;

            if (need_to_check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            StoragePtr table = nullptr;
            TableLockHolder lock;
            if (need_table)
            {
                table = tables_it->table();
                if (!table)
                    // Table might have just been removed or detached for Lazy engine (see DatabaseLazy::tryGetTable())
                    continue;

                /// The only column that requires us to hold a shared lock is data_paths as rename might alter them (on ordinary tables)
                /// and it's not protected internally by other mutexes
                static const size_t DATA_PATHS_INDEX = 5;
                if (columns_mask[DATA_PATHS_INDEX])
                {
                    lock = table->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
                    if (!lock)
                        // Table was dropped while acquiring the lock, skipping table
                        continue;
                }
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
                res_columns[res_index++]->insert(0u); // is_temporary

            if (columns_mask[src_index++])
            {
                chassert(lock != nullptr);
                Array table_paths_array;
                auto paths = table->getDataPaths();
                table_paths_array.reserve(paths.size());
                for (const String & path : paths)
                    table_paths_array.push_back(path);
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
                metadata_snapshot = table->getInMemoryMetadataPtr();

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

                if (ast_create && !context->getSettingsRef().show_table_uuid_in_table_create_query_if_not_nil)
                {
                    ast_create->uuid = UUIDHelpers::Nil;
                    ast_create->to_inner_uuid = UUIDHelpers::Nil;
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
                auto policy = table ? table->getStoragePolicy() : nullptr;
                if (policy)
                    res_columns[res_index++]->insert(policy->getName());
                else
                    res_columns[res_index++]->insertDefault();
            }

            auto settings = context->getSettingsRef();
            settings.select_sequential_consistency = 0;
            if (columns_mask[src_index++])
            {
                auto total_rows = table ? table->totalRows(settings) : std::nullopt;
                if (total_rows)
                    res_columns[res_index++]->insert(*total_rows);
                else
                    res_columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                auto total_bytes = table->totalBytes(settings);
                if (total_bytes)
                    res_columns[res_index++]->insert(*total_bytes);
                else
                    res_columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                auto total_bytes_uncompressed = table->totalBytesUncompressed(settings);
                if (total_bytes_uncompressed)
                    res_columns[res_index++]->insert(*total_bytes_uncompressed);
                else
                    res_columns[res_index++]->insertDefault();
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
                {
                    res_columns[res_index++]->insert(table_merge_tree->getTotalMarksCount());
                }
                else
                    res_columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                auto lifetime_rows = table ? table->lifetimeRows() : std::nullopt;
                if (lifetime_rows)
                    res_columns[res_index++]->insert(*lifetime_rows);
                else
                    res_columns[res_index++]->insertDefault();
            }

            if (columns_mask[src_index++])
            {
                auto lifetime_bytes = table ? table->lifetimeBytes() : std::nullopt;
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
        }
    }

    UInt64 num_rows = res_columns.at(0)->size();
    return Chunk(std::move(res_columns), num_rows);
}

Chunk DetachedTablesBlockSource::generate()
{
    if (done)
        return {};

    MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();

    const auto access = context->getAccess();
    const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    for (size_t database_idx = 0, rows_count = 0; database_idx < databases->size() && rows_count < max_block_size; ++database_idx)
    {
        database_name = databases->getDataAt(database_idx).toString();
        database = DatabaseCatalog::instance().tryGetDatabase(database_name);

        if (!database)
            continue;

        const bool need_to_check_access_for_tables
            = need_to_check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

        if (!detached_tables_it || !detached_tables_it->isValid())
            detached_tables_it = database->getDetachedTablesIterator(context, {}, false);

        for (; rows_count < max_block_size && detached_tables_it->isValid(); detached_tables_it->next())
        {
            const auto detached_table_name = detached_tables_it->table();

            if (!detached_tables.contains(detached_table_name))
                continue;

            if (need_to_check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, detached_table_name))
                continue;

            fillResultColumnsByDetachedTableIterator(result_columns);
            ++rows_count;
        }
    }

    const UInt64 num_rows = result_columns.at(0)->size();
    done = true;
    return Chunk(std::move(result_columns), num_rows);
}

void DetachedTablesBlockSource::fillResultColumnsByDetachedTableIterator(MutableColumns & result_columns) const
{
    size_t src_index = 0;
    size_t res_index = 0;

    if (columns_mask[src_index++])
        result_columns[res_index++]->insert(detached_tables_it->database());

    if (columns_mask[src_index++])
        result_columns[res_index++]->insert(detached_tables_it->table());

    if (columns_mask[src_index++])
        result_columns[res_index++]->insert(detached_tables_it->uuid());

    if (columns_mask[src_index++])
        result_columns[res_index++]->insert(detached_tables_it->metadataPath());

    if (columns_mask[src_index++])
        result_columns[res_index++]->insert(detached_tables_it->isPermanently());
}
}
