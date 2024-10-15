#include <Storages/System/StorageSystemProjections.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
StorageSystemProjections::StorageSystemProjections(const StorageID & table_id_)
    : IStorage(table_id_)
{
    auto projection_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Normal",       static_cast<UInt8>(ProjectionDescription::Type::Normal)},
            {"Aggregate",    static_cast<UInt8>(ProjectionDescription::Type::Aggregate)}
        }
    );

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            { "database", std::make_shared<DataTypeString>(), "Database name."},
            { "table", std::make_shared<DataTypeString>(), "Table name."},
            { "name", std::make_shared<DataTypeString>(), "Projection name."},
            { "type", std::move(projection_type_datatype), "Projection type."},
            { "sorting_key", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Projection sorting key."},
            { "query", std::make_shared<DataTypeString>(), "Projection query."},
        }));
    setInMemoryMetadata(storage_metadata);
}

class ProjectionsSource : public ISource
{
public:
    ProjectionsSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ContextPtr context_)
        : ISource(header)
        , column_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
        , database_idx(0)
    {}

    String getName() const override { return "Projections"; }

protected:
    Chunk generate() override
    {
        if (database_idx >= databases->size())
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx).toString();
                database = DatabaseCatalog::instance().tryGetDatabase(database_name);

                if (database)
                    break;
                ++database_idx;
            }

            if (database_idx >= databases->size())
                break;

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesIterator(context);

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                const auto table = tables_it->table();
                if (!table)
                    continue;
                StorageMetadataPtr metadata_snapshot = table->getInMemoryMetadataPtr();
                if (!metadata_snapshot)
                    continue;
                const auto & projections = metadata_snapshot->getProjections();

                for (const auto & projection : projections)
                {
                    ++rows_count;

                    size_t src_index = 0;
                    size_t res_index = 0;

                    // 'database' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(database_name);
                    // 'table' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(table_name);
                    // 'name' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(projection.name);
                    // 'type' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(projection.type);
                    // 'sorting_key' column
                    if (column_mask[src_index++])
                    {
                        auto columns = projection.metadata->getSortingKeyColumns();

                        Array sorting_key;
                        sorting_key.reserve(columns.size());
                        for (const auto & column : columns)
                        {
                            sorting_key.push_back(column);
                        }
                        res_columns[res_index++]->insert(sorting_key);
                    }
                    // 'query' column
                    if (column_mask[src_index++])
                    {
                        res_columns[res_index++]->insert(serializeAST(*projection.definition_ast->children.at(0)));
                    }
                }
            }
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
};

class ReadFromSystemProjections : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemProjections"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemProjections(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemProjections> storage_,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::move(sample_block),
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
    std::shared_ptr<StorageSystemProjections> storage;
    std::vector<UInt8> columns_mask;
    const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
};

void ReadFromSystemProjections::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }
}

void StorageSystemProjections::read(
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

    auto this_ptr = std::static_pointer_cast<StorageSystemProjections>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemProjections>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemProjections::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        /// Lazy database can contain only very primitive tables, it cannot contain tables with projections.
        /// Skip it to avoid unnecessary tables loading in the Lazy database.
        if (database->getEngineName() != "Lazy")
            column->insert(database_name);
    }

    /// Condition on "database" in a query acts like an index.
    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    if (virtual_columns_filter)
        VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, block);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;
    pipeline.init(Pipe(std::make_shared<ProjectionsSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases), context)));
}

}
