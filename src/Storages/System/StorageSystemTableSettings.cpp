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

namespace
{

DataTypePtr originEnum()
{
    DataTypeEnum8::Values values;
    for (const auto origin : magic_enum::enum_values<SettingOrigin>())
        values.emplace_back(toString(origin), static_cast<Int8>(origin));
    return std::make_shared<DataTypeEnum8>(std::move(values));
}

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

    description.add({"source", originEnum(),
        "Where the value came from. "
        "`default` - the engine's compiled-in default. "
        "`config` - a server config section, such as `<merge_tree>` or `<distributed>`. "
        "`compatibility` - rolled back to an older release's default by the `compatibility` setting. "
        "`named_collection` - a named collection the table was built from, for the settings it supplied rather than "
        "those the engine arguments overrode. "
        "`definition` - the table's own `SETTINGS` clause, whether stated in `CREATE` or written there by a later `ALTER`; "
        "the two cannot be told apart, because `ALTER ... MODIFY SETTING` rewrites the stored `CREATE` query and both "
        "are recorded as the definition. "
        "`shared_metadata` - table metadata shared between replicas, such as Keeper for `S3Queue` and `AzureQueue`, "
        "which is what the table uses even when its own `CREATE` query says otherwise. "
        "`runtime` - adjusted by the engine as it runs and never written back to its settings. Reserved: no engine "
        "reports it yet. "
        "`other` - something assigned the setting, but the engine does not say what. "
        "Which of these an engine can report depends on the engine: only `MergeTree` family tables report "
        "`compatibility`, only `S3Queue` and `AzureQueue` report `shared_metadata`, and `File`, `URL` and the "
        "plain object storage engines report only `definition`."});
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
        TablesFilter tables_filter_,
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases_cursor(std::move(databases_))
        , with_temporary_tables(with_temporary_tables_)
        , table_filter(std::move(table_filter_))
        , engine_filter(std::move(engine_filter_))
        , tables_filter(std::move(tables_filter_))
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

        /// Whether this user may see the real value of a secret setting - decided as `SHOW CREATE TABLE`
        /// decides it, so the two surfaces cannot disagree.
        const bool show_secrets = canDisplaySecrets(context);

        size_t rows_count = 0;
        SettingRowWriter writer(res_columns, column_mask);

        auto add_table = [&](const String & db_name, const String & tbl_name, const StoragePtr & table)
        {
            /// An alias must not expose the metadata of a target the user cannot see.
            if (const auto * alias = table->as<StorageAlias>();
                alias && !alias->isTargetTableGranted(context, AccessType::SHOW_TABLES, {}))
                return;

            const String engine_name = table->getName();

            /// The engine is known only once the table is, so a predicate on it is applied here - still before the
            /// settings are read, which is the expensive part.
            if (engine_filter && !engineFilterKeeps(db_name, tbl_name, engine_name))
                return;

            for (const auto & setting : table->getTableSettings(context))
            {
                const bool is_masked = isSettingValueMasked(setting, show_secrets);
                rows_count += writeSettingRows(
                    writer,
                    setting,
                    is_masked,
                    [&](SettingRowWriter & row)
                    {
                        row.put(db_name);
                        row.put(tbl_name);
                        row.put(engine_name);
                    },
                    [&](SettingRowWriter & row)
                    {
                        row.put(static_cast<Int8>(setting.origin));
                        row.put(is_masked);
                    });
            }
        };

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
                        ? database->getTablesIteratorWithHint(context, allowed, /* skip_not_loaded */ false, tables_filter)
                        : database->getTablesIterator(context, allowed));
            }

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            auto & tables_it = databases_cursor.getTablesIterator();
            for (; rows_count < max_block_size && tables_it.isValid(); tables_it.next())
            {
                auto table_name = tables_it.name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                StoragePtr table = tables_it.table();
                /// A data lake table the hinted iterator could not resolve: ask for it directly, which throws the
                /// catalog's error, or returns nothing for a table that is gone. Otherwise a table the catalog refuses
                /// to describe would silently lose its rows. The error gets the context the plain iterator gives it.
                if (!table && databases_cursor.getDatabase()->isDatalakeCatalog()
                    && context->getSettingsRef()[Setting::database_datalake_require_metadata_access])
                {
                    try
                    {
                        table = databases_cursor.getDatabase()->tryGetTable(table_name, context);
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
                if (!table)
                    continue;

                add_table(database_name, table_name, table);
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
                add_table("", external_tables_it->first, external_tables_it->second);
        }

        if (rows_count == 0)
            return {};

        return Chunk(std::move(res_columns), rows_count);
    }

private:
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

    /// Drops from the session's temporary tables, which no database lists, every one the query's `table`
    /// predicate excludes, so their settings are never read. Only that predicate: one on `database` alone
    /// builds no `table_filter` and is answered once by `with_temporary_tables`, before this is called.
    void keepTablesAllowedByFilter(Tables & tables) const
    {
        auto database_column = ColumnString::create();
        auto table_column = ColumnString::create();
        for (const auto & [table_name, storage] : tables)
        {
            database_column->insertDefault();
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
        if (tables_filter.kind == TablesFilter::Kind::Equals && !databases_cursor.getDatabase()->isDatalakeCatalog())
            return [name = tables_filter.pattern](const String & table_name) { return table_name == name; };

        auto database_column = ColumnString::create();
        auto table_column = ColumnString::create();
        for (const auto & table_details : databases_cursor.getDatabase()->getLightweightTablesIteratorWithHint(
                 context, /* filter_by_table_name */ {}, /* skip_not_loaded */ false, tables_filter))
        {
            database_column->insert(database_name);
            table_column->insert(table_details.name);
        }

        Block block
        {
            ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "database"),
            ColumnWithTypeAndName(std::move(table_column), std::make_shared<DataTypeString>(), "table"),
        };
        VirtualColumnUtils::filterBlockWithExpression(table_filter, block);

        const auto & surviving = block.getByName("table").column;
        auto allowed = std::make_shared<NameSet>();
        for (size_t i = 0; i < surviving->size(); ++i)
            allowed->insert(String{surviving->getDataAt(i)});

        return [allowed](const String & name) { return allowed->contains(name); };
    }

    std::vector<UInt8> column_mask;
    UInt64 max_block_size;
    DatabaseTablesCursor databases_cursor;
    bool with_temporary_tables;
    ExpressionActionsPtr table_filter;
    ExpressionActionsPtr engine_filter;
    TablesFilter tables_filter;
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
    ExpressionActionsPtr virtual_columns_filter;
    ExpressionActionsPtr table_filter;
    ExpressionActionsPtr engine_filter;
    TablesFilter tables_filter;
};

void ReadFromSystemTableSettings::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (!filter_actions_dag)
        return;

    /// Two of them: one to skip a database without opening it, and one to skip tables within a
    /// database. They are split separately because the first is applied to a block that has no
    /// `table` column, and an expression referring to one could not be evaluated there.
    Block databases_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
    };
    if (auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &databases_block, context))
        virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);

    Block tables_block
    {
        { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
        { ColumnString::create(), std::make_shared<DataTypeString>(), "table" },
    };
    /// Only a predicate that reads `table` can narrow a database's tables. One on `database` alone is already
    /// applied above, and listing a data lake catalog's names just to keep all of them costs a full catalog walk.
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
    tables_filter = extractTableNameFilter(filter_actions_dag->getOutputs().at(0), "table");
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
    MutableColumnPtr column = ColumnString::create();

    /// The same database selection as `system.tables` and `system.columns`, rather than the
    /// unconditional exclusion that `system.constraints`, `system.projections` and
    /// `system.data_skipping_indices` use. Those skip an external database because a table in one
    /// has no ClickHouse constraints, projections or skipping indices to report - there is genuinely
    /// nothing there. Settings are not like that: `StorageMySQL` and the data lake storages both
    /// answer `getTableSettings`, so excluding them would hide rows this table exists to show.
    const auto & settings = context->getSettingsRef();
    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{
        .with_datalake_catalogs = settings[Setting::show_data_lake_catalogs_in_system_tables],
        .with_remote_databases = settings[Setting::show_remote_databases_in_system_tables]});
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;
        column->insert(database_name);
    }

    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    if (virtual_columns_filter)
        VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, block);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;

    /// The session's temporary tables are not in any database, and report an empty `database`. Ask the same
    /// filter whether that value survives, so a query about one database does not read their settings.
    bool with_temporary_tables = true;
    if (virtual_columns_filter)
    {
        auto temporary_database_column = ColumnString::create();
        temporary_database_column->insertDefault();
        Block temporary_block
        {
            ColumnWithTypeAndName(std::move(temporary_database_column), std::make_shared<DataTypeString>(), "database"),
        };
        VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, temporary_block);
        with_temporary_tables = temporary_block.getByPosition(0).column->size() > 0;
    }

    pipeline.init(Pipe(std::make_shared<TableSettingsSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases),
        with_temporary_tables, table_filter, engine_filter, tables_filter, context)));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTableSettings) }
