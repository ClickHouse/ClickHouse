#include <Storages/System/StorageSystemTableSettings.h>
#include <Storages/System/extractTableNameFilter.h>
#include <Storages/System/SettingsTableColumns.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageAlias.h>
#include <Storages/System/DatabaseTablesCursor.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <base/EnumReflection.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsBool show_data_lake_catalogs_in_system_tables;
    extern const SettingsBool database_datalake_require_metadata_access;
    extern const SettingsBool show_remote_databases_in_system_tables;
}


StorageSystemTableSettings::StorageSystemTableSettings(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription());
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

ColumnsDescription StorageSystemTableSettings::getColumnsDescription()
{
    ColumnsDescription description
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the table."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            "Engine of the table. Setting names are engine-specific, so the same name can mean different things for different engines."},
    };

    for (const auto & column : sharedSettingColumns())
        description.add(column);

    description.add({"is_masked", std::make_shared<DataTypeUInt8>(),
        "1 if `value` is a placeholder rather than the real value, because the setting holds a secret and the current "
        "user may not see it. Grant `displaySecretsInShowAndSelect` and enable "
        "`format_display_secrets_in_show_and_select` to see it."});

    return description;
}

VirtualColumnsDescription StorageSystemTableSettings::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

class TableSettingsSource : public ISource
{
public:
    TableSettingsSource(
        std::vector<UInt8> columns_mask_,
        SharedHeader header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        bool with_temporary_tables_,
        ExpressionActionsPtr table_filter_,
        ExpressionActionsPtr engine_filter_,
        TablesFilter catalog_name_hint_,
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases_cursor(std::move(databases_))
        , with_temporary_tables(with_temporary_tables_)
        , table_filter(std::move(table_filter_))
        , engine_filter(std::move(engine_filter_))
        , catalog_name_hint(std::move(catalog_name_hint_))
        , context(Context::createCopy(context_))
    {
    }

    String getName() const override { return "TableSettings"; }

protected:
    Chunk generate() override
    {
        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        /// Whether this user may see the real value of a secret setting - decided as `SHOW CREATE TABLE`
        /// decides it, so the two surfaces cannot disagree.
        SettingRowWriter writer(res_columns, column_mask, canDisplaySecrets(context));

        /// Phase 1: catalog databases
        while (rows_count < max_block_size)
        {
            if (!databases_cursor.advanceToNextDatabase())
                break;

            const String & database_name = databases_cursor.getDatabaseName();

            if (!databases_cursor.hasTablesIterator())
            {
                const auto & database = databases_cursor.getDatabase();
                auto allowed = tablesAllowedIn(database_name);
                /// A data lake catalog lists its tables over the network, and only the hinted iterator passes the
                /// table-name hint on, as `system.tables` does - the plain one walks the whole catalog to resolve a
                /// single table. The hinted one hands back a table it could not resolve as a null storage rather
                /// than failing; the loop below restores the plain iterator's outcome for those. Other databases
                /// keep the plain iterator: for `Remote` the hinted one would turn an unreachable server from no
                /// rows into an error.
                databases_cursor.setTablesIterator(
                    database->isDatalakeCatalog()
                        ? database->getTablesIteratorWithHint(context, allowed, /* skip_not_loaded */ false, catalog_name_hint)
                        : database->getTablesIterator(context, allowed));
            }

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            auto & tables_it = databases_cursor.getTablesIterator();
            for (; rows_count < max_block_size && tables_it.isValid(); tables_it.next())
            {
                auto table_name = tables_it.name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                if (const auto table = resolveTable(tables_it.table(), table_name))
                    rows_count += emitTable(writer, database_name, table_name, table);
            }
        }

        /// Phase 2: session temporary tables, once all catalog databases are consumed. They get the same two
        /// filters the catalog tables get, because one table's settings are hundreds of rows: a predicate on
        /// `database` decides whether any of them can match at all - they report an empty one, which
        /// `with_temporary_tables` answers once - and a predicate on `table` is applied here, before the
        /// settings are read rather than after.
        if (with_temporary_tables && rows_count < max_block_size)
        {
            if (!external_tables_initialized)
            {
                external_tables_initialized = true;
                if (context->hasSessionContext())
                    external_tables = context->getSessionContext()->getExternalTables();
                if (table_filter && !external_tables.empty())
                    keepTablesAllowedByFilter(external_tables);
                external_tables_it = external_tables.begin();
            }

            for (; rows_count < max_block_size && external_tables_it != external_tables.end(); ++external_tables_it)
                rows_count += emitTable(writer, "", external_tables_it->first, external_tables_it->second);
        }

        if (rows_count == 0)
            return {};

        return Chunk(std::move(res_columns), rows_count);
    }

private:
    /// The storage to report, or nothing where there is none to report. A data lake table the hinted iterator could
    /// not resolve comes back null: ask the catalog for it directly, which throws its error, or returns nothing for a
    /// table that is gone. Otherwise a table the catalog refuses to describe would silently lose its rows. The error
    /// gets the context the plain iterator gives it.
    StoragePtr resolveTable(StoragePtr table, const String & table_name) const
    {
        if (table || !databases_cursor.getDatabase()->isDatalakeCatalog()
            || !context->getSettingsRef()[Setting::database_datalake_require_metadata_access])
            return table;

        try
        {
            return databases_cursor.getDatabase()->tryGetTable(table_name, context);
        }
        catch (Exception & e)
        {
            e.addMessage(
                "while fetching table metadata for existing table '{}'. If you want this error to be ignored, "
                "use database_datalake_require_metadata_access=0",
                table_name);
            throw;
        }
    }

    /// Writes one table's settings, a row per setting and per alias of one. Returns how many rows that was - none
    /// for a table the query turns out not to want, which is decided here because it takes the engine.
    size_t emitTable(SettingRowWriter & writer, const String & db_name, const String & tbl_name, const StoragePtr & table) const
    {
        /// An alias must not expose the metadata of a target the user cannot see.
        if (const auto * alias = table->as<StorageAlias>();
            alias && !alias->isTargetTableGranted(context, AccessType::SHOW_TABLES, {}))
            return 0;

        const String engine_name = table->getName();

        /// The engine is known only once the table is, so a predicate on it is applied here - still before the
        /// settings are read, which is the expensive part.
        if (engine_filter && !engineFilterKeeps(db_name, tbl_name, engine_name))
            return 0;

        size_t rows = 0;
        for (const auto & setting : table->getTableSettings(context))
            rows += writeSettingRows(
                writer,
                setting,
                [&](SettingRowWriter & row)
                {
                    row.put(db_name);
                    row.put(tbl_name);
                    row.put(engine_name);
                },
                [&](SettingRowWriter & row) { row.put(row.masks(setting)); });
        return rows;
    }

    /// Whether the query's predicate on `engine` - which may involve `database` and `table` too - keeps this table.
    bool engineFilterKeeps(const String & db_name, const String & tbl_name, const String & engine_name) const
    {
        auto database_column = ColumnString::create();
        database_column->insert(db_name);
        auto table_column = ColumnString::create();
        table_column->insert(tbl_name);
        const auto engine_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        auto engine_column = engine_type->createColumn();
        engine_column->insert(engine_name);

        Block block
        {
            ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "database"),
            ColumnWithTypeAndName(std::move(table_column), std::make_shared<DataTypeString>(), "table"),
            ColumnWithTypeAndName(std::move(engine_column), engine_type, "engine"),
        };
        VirtualColumnUtils::filterBlockWithExpression(engine_filter, block);
        return block.rows() > 0;
    }

    /// Which of `table_names`, all of database `database_name`, the query's `table` predicate keeps. The names go
    /// through the filter in one block, so a table's settings are never read to be discarded afterwards.
    NameSet tableNamesAllowedByFilter(const String & database_name, const Strings & table_names) const
    {
        auto database_column = ColumnString::create();
        auto table_column = ColumnString::create();
        for (const auto & table_name : table_names)
        {
            database_column->insert(database_name);
            table_column->insert(table_name);
        }

        Block block
        {
            ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "database"),
            ColumnWithTypeAndName(std::move(table_column), std::make_shared<DataTypeString>(), "table"),
        };
        VirtualColumnUtils::filterBlockWithExpression(table_filter, block);

        const auto & surviving = block.getByName("table").column;
        NameSet allowed;
        for (size_t i = 0; i < surviving->size(); ++i)
            allowed.insert(String{surviving->getDataAt(i)});
        return allowed;
    }

    /// Drops from the session's temporary tables, which no database lists, every one the query's `table`
    /// predicate excludes. Only that predicate: one on `database` alone builds no `table_filter` and is answered
    /// once by `with_temporary_tables`, before this is called.
    void keepTablesAllowedByFilter(Tables & tables) const
    {
        Strings names;
        names.reserve(tables.size());
        for (const auto & [table_name, storage] : tables)
            names.push_back(table_name);

        const auto allowed = tableNamesAllowedByFilter(/* database_name */ "", names);
        std::erase_if(tables, [&allowed](const auto & entry) { return !allowed.contains(entry.first); });
    }

    /// Which of a database's tables the query can still be about. A table's settings are hundreds of rows, so
    /// `WHERE table = ...` - the query `SHOW TABLE SETTINGS` generates - must not read every table to discard
    /// the rest: the names are filtered first, and the real iterator is asked only for the survivors.
    ///
    /// The names come from `getLightweightTablesIterator`, not `getTablesIterator`: for an external database the
    /// latter already resolves storages (`DatabaseRemote::fetchTable`, `DatabaseDataLake::tryGetTableImpl`), so
    /// listing names through it would open every table and let one unresolvable table fail the lookup.
    IDatabase::FilterByNameFunction tablesAllowedIn(const String & database_name) const
    {
        if (!table_filter)
            return {};

        /// `WHERE table = '...'` can match only the table it names, so there is nothing to list, and listing a
        /// `PostgreSQL` database fetches the structure of every table in it. The rest of the filter is applied to the
        /// rows afterwards.
        if (catalog_name_hint.kind == TablesFilter::Kind::Equals && !databases_cursor.getDatabase()->isDatalakeCatalog())
            return [name = catalog_name_hint.pattern](const String & table_name) { return table_name == name; };

        Strings names;
        for (const auto & table_details : databases_cursor.getDatabase()->getLightweightTablesIteratorWithHint(
                 context, /* filter_by_table_name */ {}, /* skip_not_loaded */ false, catalog_name_hint))
            names.push_back(table_details.name);

        auto allowed = std::make_shared<NameSet>(tableNamesAllowedByFilter(database_name, names));
        return [allowed](const String & name) { return allowed->contains(name); };
    }

    std::vector<UInt8> column_mask;
    UInt64 max_block_size;
    DatabaseTablesCursor databases_cursor;
    bool with_temporary_tables;
    ExpressionActionsPtr table_filter;
    ExpressionActionsPtr engine_filter;
    TablesFilter catalog_name_hint;
    ContextPtr context;
    Tables external_tables;
    Tables::const_iterator external_tables_it;
    bool external_tables_initialized = false;
};

class ReadFromSystemTableSettings : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemTableSettings"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemTableSettings(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemTableSettings> storage_,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(std::move(sample_block)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<StorageSystemTableSettings> storage;
    std::vector<UInt8> columns_mask;
    const size_t max_block_size;
    ExpressionActionsPtr table_filter;
    ExpressionActionsPtr engine_filter;
    TablesFilter catalog_name_hint;
};

void ReadFromSystemTableSettings::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (!filter_actions_dag)
        return;

    /// A predicate that reads `table` narrows a database's tables; one on `database` alone is applied to the
    /// database list itself, in `initializePipeline`.

    Block tables_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "table" },
    };
    /// Only a predicate that reads `table` can narrow a database's tables: listing a data lake catalog's names just
    /// to keep all of them costs a full catalog walk.
    if (auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &tables_block, context);
        dag && std::ranges::any_of(dag->getInputs(), [](const auto * input) { return input->result_name == "table"; }))
        table_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);

    /// And one that reads `engine`, applied to each table once it is resolved - `system.tables` pushes it down too.
    Block engines_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "table" },
        { ColumnString::create(), std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "engine" },
    };
    if (auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &engines_block, context);
        dag && std::ranges::any_of(dag->getInputs(), [](const auto * input) { return input->result_name == "engine"; }))
        engine_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);

    /// A namespace-pushdown hint for catalogs that can restrict what they list server-side. The
    /// table name lives in the `table` column here - `name` is the setting's name - so that is the
    /// column the hint has to be read from.
    catalog_name_hint = extractTableNameFilter(filter_actions_dag->getOutputs().at(0), "table");
}

void StorageSystemTableSettings::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t /* num_streams */)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, header] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto this_ptr = std::static_pointer_cast<StorageSystemTableSettings>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemTableSettings>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemTableSettings::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & filter_dag = getFilterActionsDAG();
    const ActionsDAG::Node * predicate = filter_dag ? filter_dag->getOutputs().at(0) : nullptr;

    /// The same databases `system.tables` selects, through the same helper, rather than the unconditional exclusion
    /// that `system.constraints`, `system.projections` and `system.data_skipping_indices` use. Those skip an external
    /// database because a table in one has no ClickHouse constraints, projections or skipping indices to report -
    /// there is genuinely nothing there. Settings are not like that: `StorageMySQL` and the data lake storages both
    /// answer `getTableSettings`, so excluding them would hide rows this table exists to show.
    auto filtered_databases = detail::getFilteredDatabases(predicate, context);

    /// The session's temporary tables are not in any database, and report an empty `database`. Ask the same
    /// predicate whether that value survives, so a query about one database does not read their settings.
    auto temporary_database_column = ColumnString::create();
    temporary_database_column->insertDefault();
    Block temporary_block
    {
        ColumnWithTypeAndName(std::move(temporary_database_column), std::make_shared<DataTypeString>(), "database"),
    };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, temporary_block, context);
    const bool with_temporary_tables = temporary_block.rows() > 0;

    pipeline.init(Pipe(std::make_shared<TableSettingsSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases),
        with_temporary_tables, table_filter, engine_filter, catalog_name_hint, context)));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTableSettings) }
