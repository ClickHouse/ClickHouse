#include <Storages/System/StorageSystemConstraints.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

StorageSystemConstraints::StorageSystemConstraints(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    auto constraint_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"CHECK",  static_cast<Int8>(ASTConstraintDeclaration::Type::CHECK)},
            {"ASSUME", static_cast<Int8>(ASTConstraintDeclaration::Type::ASSUME)},
        }
    );

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"name", std::make_shared<DataTypeString>(), "Constraint name."},
        {"type", std::move(constraint_type_datatype), "Constraint type."},
        {"expression", std::make_shared<DataTypeString>(), "Constraint expression."},
    }));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemConstraints::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

class ConstraintsSource : public ISource
{
public:
    ConstraintsSource(
        std::vector<UInt8> columns_mask_,
        SharedHeader header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
        , database_idx(0)
    {
    }

    String getName() const override { return "Constraints"; }

protected:
    Chunk generate() override
    {
        if (database_idx > databases->size())
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;

        auto add_constraints = [&](const String & db_name, const String & tbl_name, const StoragePtr & table)
        {
            StorageMetadataPtr metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
            if (!metadata_snapshot)
                return;
            const auto & constraints = metadata_snapshot->getConstraints();
            if (constraints.empty())
                return;

            for (const auto & constraint_ast : constraints.getConstraints())
            {
                const auto * decl = constraint_ast->as<ASTConstraintDeclaration>();
                if (!decl)
                    continue;

                ++rows_count;

                size_t src_index = 0;
                size_t res_index = 0;

                if (column_mask[src_index++])
                    res_columns[res_index++]->insert(db_name);
                if (column_mask[src_index++])
                    res_columns[res_index++]->insert(tbl_name);
                if (column_mask[src_index++])
                    res_columns[res_index++]->insert(decl->name);
                if (column_mask[src_index++])
                    res_columns[res_index++]->insert(decl->type);
                if (column_mask[src_index++])
                    res_columns[res_index++]->insert(decl->expr->formatForLogging());
            }
        };

        /// Phase 1: catalog databases
        while (database_idx < databases->size() && rows_count < max_block_size)
        {
            if (!tables_it || !tables_it->isValid())
            {
                while (database_idx < databases->size())
                {
                    database_name = databases->getDataAt(database_idx);
                    database = DatabaseCatalog::instance().tryGetDatabase(database_name);
                    if (database)
                    {
                        tables_it = database->getTablesIterator(context);
                        break;
                    }
                    ++database_idx;
                }
                if (database_idx == databases->size())
                    break;
            }

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                const auto table = tables_it->table();
                if (!table)
                    continue;

                add_constraints(database_name, table_name, table);
            }

            if (!tables_it->isValid())
                ++database_idx;
        }

        /// Phase 2: session temporary tables
        if (database_idx == databases->size() && rows_count < max_block_size)
        {
            if (!external_tables_initialized)
            {
                external_tables_initialized = true;
                if (context->hasSessionContext())
                    external_tables = context->getSessionContext()->getExternalTables();
                external_tables_it = external_tables.begin();
            }

            for (; rows_count < max_block_size && external_tables_it != external_tables.end(); ++external_tables_it)
                add_constraints("", external_tables_it->first, external_tables_it->second);

            if (external_tables_it == external_tables.end())
                ++database_idx;
        }

        return Chunk(std::move(res_columns), rows_count);
    }

private:
    std::vector<UInt8> column_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    ContextPtr context;
    size_t database_idx;
    DatabasePtr database;
    std::string database_name;
    DatabaseTablesIteratorPtr tables_it;
    Tables external_tables;
    Tables::const_iterator external_tables_it;
    bool external_tables_initialized = false;
};

class ReadFromSystemConstraints : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemConstraints"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemConstraints(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemConstraints> storage_,
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
    std::shared_ptr<StorageSystemConstraints> storage;
    std::vector<UInt8> columns_mask;
    const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
};

void ReadFromSystemConstraints::applyFilters(ActionDAGNodes added_filter_nodes)
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

void StorageSystemConstraints::readImpl(
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

    auto this_ptr = std::static_pointer_cast<StorageSystemConstraints>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemConstraints>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemConstraints::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});
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
    pipeline.init(Pipe(std::make_shared<ConstraintsSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases), context)));
}

}
