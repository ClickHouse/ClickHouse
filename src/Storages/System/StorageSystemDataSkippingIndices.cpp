#include <Storages/System/StorageSystemDataSkippingIndices.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
StorageSystemDataSkippingIndices::StorageSystemDataSkippingIndices(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            { "database", std::make_shared<DataTypeString>(), "Database name."},
            { "table", std::make_shared<DataTypeString>(), "Table name."},
            { "name", std::make_shared<DataTypeString>(), "Index name."},
            { "type", std::make_shared<DataTypeString>(), "Index type."},
            { "type_full", std::make_shared<DataTypeString>(), "Index type expression from create statement."},
            { "expr", std::make_shared<DataTypeString>(), "Expression for the index calculation."},
            { "granularity", std::make_shared<DataTypeUInt64>(), "The number of granules in the block."},
            { "data_compressed_bytes", std::make_shared<DataTypeUInt64>(), "The size of compressed data, in bytes."},
            { "data_uncompressed_bytes", std::make_shared<DataTypeUInt64>(), "The size of decompressed data, in bytes."},
            { "marks", std::make_shared<DataTypeUInt64>(), "The size of marks, in bytes."}
        }));
    setInMemoryMetadata(storage_metadata);
}

class DataSkippingIndicesSource : public ISource
{
public:
    DataSkippingIndicesSource(
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

    String getName() const override { return "DataSkippingIndices"; }

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
                const auto indices = metadata_snapshot->getSecondaryIndices();

                auto secondary_index_sizes = table->getSecondaryIndexSizes();
                for (const auto & index : indices)
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
                        res_columns[res_index++]->insert(index.name);
                    // 'type' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(index.type);
                    // 'type_full' column
                    if (column_mask[src_index++])
                    {
                        auto * expression = index.definition_ast->as<ASTIndexDeclaration>();
                        auto index_type = expression ? expression->getType() : nullptr;
                        if (index_type)
                            res_columns[res_index++]->insert(queryToString(*index_type));
                        else
                            res_columns[res_index++]->insertDefault();
                    }
                    // 'expr' column
                    if (column_mask[src_index++])
                    {
                        if (auto expression = index.expression_list_ast)
                            res_columns[res_index++]->insert(queryToString(expression));
                        else
                            res_columns[res_index++]->insertDefault();
                    }
                    // 'granularity' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(index.granularity);

                    auto & secondary_index_size = secondary_index_sizes[index.name];

                    // 'compressed bytes' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(secondary_index_size.data_compressed);

                    // 'uncompressed bytes' column

                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(secondary_index_size.data_uncompressed);

                    /// 'marks' column
                    if (column_mask[src_index++])
                        res_columns[res_index++]->insert(secondary_index_size.marks);
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

class ReadFromSystemDataSkippingIndices : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemDataSkippingIndices"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemDataSkippingIndices(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemDataSkippingIndices> storage_,
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
    std::shared_ptr<StorageSystemDataSkippingIndices> storage;
    std::vector<UInt8> columns_mask;
    const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
};

void ReadFromSystemDataSkippingIndices::applyFilters(ActionDAGNodes added_filter_nodes)
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

void StorageSystemDataSkippingIndices::read(
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

    auto this_ptr = std::static_pointer_cast<StorageSystemDataSkippingIndices>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemDataSkippingIndices>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemDataSkippingIndices::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        /// Lazy database can contain only very primitive tables,
        /// it cannot contain tables with data skipping indices.
        /// Skip it to avoid unnecessary tables loading in the Lazy database.
        if (database->getEngineName() != "Lazy")
            column->insert(database_name);
    }

    /// Condition on "database" in a query acts like an index.
    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    if (virtual_columns_filter)
        VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, block);

    ColumnPtr & filtered_databases = block.getByPosition(0).column;
    pipeline.init(Pipe(std::make_shared<DataSkippingIndicesSource>(
        std::move(columns_mask), getOutputHeader(), max_block_size, std::move(filtered_databases), context)));
}

}
