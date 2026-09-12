#include <Storages/System/StorageSystemTableSettings.h>
#include <Storages/System/extractTableNameFilter.h>

#include <Access/ContextAccess.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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

namespace DB
{

namespace Setting
{
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsBool show_data_lake_catalogs_in_system_tables;
    extern const SettingsBool show_remote_databases_in_system_tables;
}

namespace
{

DataTypePtr originEnum()
{
    return std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"default", static_cast<Int8>(TableSettingOrigin::Default)},
        {"config", static_cast<Int8>(TableSettingOrigin::Config)},
        {"compatibility", static_cast<Int8>(TableSettingOrigin::Compatibility)},
        {"definition", static_cast<Int8>(TableSettingOrigin::Definition)},
        {"named_collection", static_cast<Int8>(TableSettingOrigin::NamedCollection)},
        {"shared_metadata", static_cast<Int8>(TableSettingOrigin::SharedMetadata)},
        {"runtime", static_cast<Int8>(TableSettingOrigin::Runtime)},
        {"other", static_cast<Int8>(TableSettingOrigin::Other)},
    });
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
    return ColumnsDescription
    {
        /// Which table this row is about.
        {"database", std::make_shared<DataTypeString>(), "Database of the table."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            "Engine of the table. Setting names are engine-specific, so the same name can mean different things for different engines."},

        /// The same columns as `system.merge_tree_settings`, in the same order and with the same
        /// meanings, so that what a reader knows about that table carries over to this one.
        {"name", std::make_shared<DataTypeString>(), "Setting name."},
        {"value", std::make_shared<DataTypeString>(),
            "Value the table uses. Unlike `SHOW CREATE TABLE`, this is the value in effect, which may come from a named collection, "
            "from replicated metadata, or from the engine adjusting it while running, and so need not be the value the `CREATE` query states."},
        {"default", std::make_shared<DataTypeString>(),
            "Value the setting has when nothing sets it. Empty for a setting known only from the table's `SETTINGS` clause, "
            "because an engine that keeps no settings struct has no default to report."},
        {"changed", std::make_shared<DataTypeUInt8>(), "1 if `source` is anything other than `default`."},
        {"description", std::make_shared<DataTypeString>(), "Setting description. Empty when the engine keeps no settings struct to describe it."},
        {"min", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Minimum the current user's settings constraints allow, or NULL if none is set. Only `MergeTree` settings can be constrained."},
        {"max", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Maximum the current user's settings constraints allow, or NULL if none is set. Only `MergeTree` settings can be constrained."},
        {"disallowed_values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Values the current user's settings constraints forbid. Empty when none are."},
        {"readonly", std::make_shared<DataTypeUInt8>(),
            "1 if a constraint, or the engine itself, makes the setting read-only; 0 if nothing does. Says nothing about whether the "
            "engine accepts `ALTER TABLE ... MODIFY SETTING` at all, nor about the user's `ALTER` privileges."},
        {"type", std::make_shared<DataTypeString>(), "Setting type. Empty when the engine keeps no settings struct."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "1 if the setting is obsolete."},
        {"tier", getSettingsTierEnum(),
            "Support level of the setting. Reported as `Production` for a setting known only from the table's `SETTINGS` clause, "
            "where the engine keeps no settings struct to say otherwise - such rows have an empty `default`, `type` and `description` too."},

        /// As in `system.settings`.
        {"alias_for", std::make_shared<DataTypeString>(),
            "Empty on a setting's own row. A setting writable under more than one name also gets a row per other name, "
            "carrying the same values, with this naming the one it is declared under."},

        /// Particular to this table.
        {"source", originEnum(),
            "Where the value came from. "
            "`default` - the engine's compiled-in default. "
            "`config` - a server config section, such as `<merge_tree>` or `<distributed>`. "
            "`compatibility` - rolled back to an older release's default by the `compatibility` setting. "
            "`definition` - the table's own `SETTINGS` clause, whether stated in `CREATE` or written there by a later `ALTER`; "
            "the two cannot be told apart, because `ALTER ... MODIFY SETTING` rewrites the stored `CREATE` query. "
            "`named_collection` - a named collection referenced in the engine arguments. "
            "`shared_metadata` - table metadata shared between replicas, such as Keeper for `S3Queue` and `AzureQueue`, "
            "which is what the table uses even when its own `CREATE` query says otherwise. "
            "`runtime` - adjusted by the engine as it runs and never written back to its settings. Reserved: no engine "
            "reports it yet. "
            "`other` - something assigned the setting, but the engine does not say what. "
            "Which of these an engine can report depends on the engine: only `MergeTree` family tables report `config` "
            "and `compatibility`, only `S3Queue` and `AzureQueue` report `shared_metadata`, and an engine that keeps no "
            "settings struct reports only `definition`."},
        {"is_masked", std::make_shared<DataTypeUInt8>(),
            "1 if `value` is a placeholder rather than the real value, because the setting holds a secret and the current user may not see it. "
            "Grant `displaySecretsInShowAndSelect` and enable `format_display_secrets_in_show_and_select` to see it."},
    };
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
        ExpressionActionsPtr table_filter_,
        TablesFilter tables_filter_,
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases_cursor(std::move(databases_))
        , table_filter(std::move(table_filter_))
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

        /// Whether this user may see the real value of a secret setting. The same three conditions
        /// `SHOW CREATE TABLE` uses, so the two surfaces cannot disagree.
        const bool show_secrets = context->displaySecretsInShowAndSelect()
            && context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select]
            && access->isGranted(AccessType::displaySecretsInShowAndSelect);

        size_t rows_count = 0;

        auto add_table = [&](const String & db_name, const String & tbl_name, const StoragePtr & table)
        {
            /// An alias must not expose the metadata of a target the user cannot see.
            if (const auto * alias = table->as<StorageAlias>();
                alias && !alias->isTargetTableGranted(context, AccessType::SHOW_TABLES, {}))
                return;

            const String engine_name = table->getName();

            for (const auto & setting : table->getTableSettings(context))
            {
                String value = setting.value;
                const bool is_masked = !show_secrets && !setting.masked_value.empty();
                if (is_masked)
                    value = setting.masked_value;

                /// A setting that answers to more than one name gets a row per name, as
                /// `system.settings` does, so that looking it up by the name you happen to know
                /// finds it. The rows carry the same values; `alias_for` tells them apart.
                auto add_row = [&](std::string_view name, std::string_view alias_for)
                {
                    ++rows_count;

                    size_t src_index = 0;
                    size_t res_index = 0;

                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(db_name);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(tbl_name);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(engine_name);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(name);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(value);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.default_value);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.origin != TableSettingOrigin::Default);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.description);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.min_value ? Field(*setting.min_value) : Field());
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.max_value ? Field(*setting.max_value) : Field());
                    if (column_mask[src_index++])
                    {
                        Array disallowed;
                        disallowed.reserve(setting.disallowed_values.size());
                        for (const auto & disallowed_value : setting.disallowed_values)
                            disallowed.emplace_back(disallowed_value);
                        res_columns[res_index++]->insert(disallowed);
                    }
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.readonly);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.type);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.tier == SettingsTierType::OBSOLETE);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.tier);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(alias_for);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(static_cast<Int8>(setting.origin));
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(is_masked);
                };

                add_row(setting.name, "");
                for (const auto alias : setting.aliases)
                    add_row(alias, setting.name);
            }
        };

        /// Phase 1: catalog databases
        while (rows_count < max_block_size)
        {
            if (!databases_cursor.advanceToNextDatabase())
                break;

            const String & database_name = databases_cursor.getDatabaseName();

            if (!databases_cursor.hasTablesIterator())
                databases_cursor.setTablesIterator(
                    databases_cursor.getDatabase()->getTablesIterator(context, tablesAllowedIn(database_name)));

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            auto & tables_it = databases_cursor.getTablesIterator();
            for (; rows_count < max_block_size && tables_it.isValid(); tables_it.next())
            {
                auto table_name = tables_it.name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                const auto table = tables_it.table();
                if (!table)
                    continue;

                add_table(database_name, table_name, table);
            }
        }

        /// Phase 2: session temporary tables, once all catalog databases are consumed.
        if (rows_count < max_block_size)
        {
            if (!external_tables_initialized)
            {
                external_tables_initialized = true;
                if (context->hasSessionContext())
                    external_tables = context->getSessionContext()->getExternalTables();
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
    /// Which of a database's tables the query can still be about. A table's settings are hundreds of
    /// rows, so answering `WHERE table = ...` by reading every table and discarding the rest is not
    /// affordable - and that is the query `SHOW TABLE SETTINGS` generates. So the names are filtered
    /// first and the real iterator is then asked only for what survived.
    ///
    /// The names must come from `getLightweightTablesIterator`, not `getTablesIterator`: for an
    /// external database the latter is already the storage-resolving path - `DatabaseRemote` calls
    /// `fetchTable` per listed table and `DatabaseDataLake` calls `tryGetTableImpl` per readable
    /// catalog table - so listing names through it would open every table in the database just to
    /// learn its name, and then open the survivor a second time. It also propagates failures, which
    /// would let one unrelated unresolvable table fail a single-table lookup. The lightweight
    /// iterator lists names only, and both databases apply the filter this returns before resolving
    /// anything, so only the surviving tables are ever opened.
    IDatabase::FilterByNameFunction tablesAllowedIn(const String & database_name) const
    {
        if (!table_filter)
            return {};

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
    ExpressionActionsPtr table_filter;
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
    if (auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &tables_block, context))
        table_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);

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
    pipeline.init(Pipe(std::make_shared<TableSettingsSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases),
        table_filter, tables_filter, context)));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTableSettings) }
