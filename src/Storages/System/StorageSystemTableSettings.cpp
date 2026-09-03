#include <Storages/System/StorageSystemTableSettings.h>

#include <Access/ContextAccess.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/maskSettingValue.h>
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
        {"database", std::make_shared<DataTypeString>(), "Database of the table."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"engine", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            "Engine of the table. Setting names are engine-specific, so the same name can mean different things for different engines."},
        {"name", std::make_shared<DataTypeString>(), "Setting name."},
        {"default", std::make_shared<DataTypeString>(),
            "Value the setting has when nothing sets it. Empty for a setting known only from the table's `SETTINGS` clause, "
            "because an engine that keeps no settings struct has no default to report."},
        {"value", std::make_shared<DataTypeString>(),
            "Value the table uses. Unlike `SHOW CREATE TABLE`, this is the value in effect, which may come from a named collection, "
            "from replicated metadata, or from the engine adjusting it while running, and so need not be the value the `CREATE` query states."},
        {"changed", std::make_shared<DataTypeUInt8>(), "1 if `source` is anything other than `default`."},
        {"source", originEnum(), "Where the value came from."},
        {"is_masked", std::make_shared<DataTypeUInt8>(),
            "1 if `value` is a placeholder rather than the real value, because the setting holds a secret and the current user may not see it. "
            "Grant `displaySecretsInShowAndSelect` and enable `format_display_secrets_in_show_and_select` to see it."},
        {"description", std::make_shared<DataTypeString>(), "Setting description. Empty when the engine keeps no settings struct to describe it."},
        {"type", std::make_shared<DataTypeString>(), "Setting type. Empty when the engine keeps no settings struct."},
        {"alias_for", std::make_shared<DataTypeString>(),
            "Empty on a setting's own row. A setting writable under more than one name also gets a row per other name, "
            "carrying the same values, with this naming the one it is declared under."},
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
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases_cursor(std::move(databases_))
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
                bool is_masked = false;
                if (!show_secrets)
                {
                    if (auto masked = maskSettingValue(engine_name, setting.name, value))
                    {
                        value = std::move(*masked);
                        is_masked = true;
                    }
                }

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
                        res_columns[res_index++]->insert(setting.default_value);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(value);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.origin != TableSettingOrigin::Default);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(static_cast<Int8>(setting.origin));
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(is_masked);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.description);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(setting.type);
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(alias_for);
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
                databases_cursor.setTablesIterator(databases_cursor.getDatabase()->getTablesIterator(context));

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
    std::vector<UInt8> column_mask;
    UInt64 max_block_size;
    DatabaseTablesCursor databases_cursor;
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
};

void ReadFromSystemTableSettings::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }
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

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;
        if (database->isExternal())
            continue;
        column->insert(database_name);
    }

    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    if (virtual_columns_filter)
        VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, block);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;
    pipeline.init(Pipe(std::make_shared<TableSettingsSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases), context)));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTableSettings) }
