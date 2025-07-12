#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemDatabaseReplicas.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseReplicated.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>


namespace DB
{

class SystemDatabaseReplicasSource : public ISource
{
public:
    SystemDatabaseReplicasSource(
        Block header_,
        size_t max_block_size_,
        ColumnPtr col_database_,
        ColumnPtr col_readonly_,
        ContextPtr context_)
        : ISource(header_)
        , max_databases(col_database_->size())
        , max_block_size(max_block_size_)
        , col_database(std::move(col_database_))
        , col_readonly(std::move(col_readonly_))
        , context(std::move(context_))
    {
    }

    String getName() const override { return "SystemDatabaseReplicas"; }

protected:
    Chunk generate() override;

private:
    const size_t max_databases;
    const size_t max_block_size;
    ColumnPtr col_database;
    ColumnPtr col_readonly;
    ContextPtr context;
    size_t index = 0;
};

Chunk SystemDatabaseReplicasSource::generate()
{
    if (index == max_databases)
        return {};

    MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

    for (size_t row_count{}; index < max_databases && row_count < max_block_size; index++, row_count++)
    {
        res_columns[0]->insert((*col_database)[index]);
        res_columns[1]->insert((*col_readonly)[index]);
    }

    UInt64 num_rows = res_columns.at(0)->size();
    return Chunk(std::move(res_columns), num_rows);
}


class ReadFromSystemDatabaseReplicas : public SourceStepWithFilter
{
public:
    ReadFromSystemDatabaseReplicas(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::map<String, DatabasePtr> replicated_databases_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , replicated_databases(std::move(replicated_databases_))
        , max_block_size(max_block_size_)
    {
    }

    std::string getName() const override { return "ReadFromSystemDatabaseReplicas"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::map<String, DatabasePtr> replicated_databases;
    const size_t max_block_size;
    ExpressionActionsPtr virtual_columns_filter;
};

void ReadFromSystemDatabaseReplicas::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { ColumnString::create(), std::make_shared<DataTypeString>(), "database" },
            { ColumnString::create(), std::make_shared<DataTypeUInt8>(), "is_readonly" },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }
}

void ReadFromSystemDatabaseReplicas::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto header = getOutputHeader();

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_readonly_mut = ColumnUInt8::create();

    for (auto & [db_name, data] : replicated_databases)
    {
        col_database_mut->insert(db_name);
        col_readonly_mut->insert(data->isReadOnly());
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_readonly = std::move(col_readonly_mut);

    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_readonly, std::make_shared<DataTypeUInt8>(), "is_readonly" },
        };

        if (virtual_columns_filter)
            VirtualColumnUtils::filterBlockWithExpression(virtual_columns_filter, filtered_block);

        if (!filtered_block.rows())
        {
            auto source = std::make_shared<NullSource>(std::move(header));
            pipeline.init(Pipe(std::move(source)));
            return;
        }

        col_database = filtered_block.getByName("database").column;
        col_readonly = filtered_block.getByName("is_readonly").column;
    }

    pipeline.init(Pipe(std::make_shared<SystemDatabaseReplicasSource>(header, max_block_size, col_database, col_readonly, context)));
}

StorageSystemDatabaseReplicas::StorageSystemDatabaseReplicas(const StorageID & table_id_)
    : IStorage(table_id_)
{

    ColumnsDescription description = {
        { "database", std::make_shared<DataTypeString>(),   "Database name."},
        { "is_readonly", std::make_shared<DataTypeUInt8>(),   "is_readonly"}
    };

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(description);
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemDatabaseReplicas::read(
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

    const auto access = context->getAccess();
    const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    std::map<String, DatabasePtr> replicated_databases;
    for (const auto & [db_name, db_data] : DatabaseCatalog::instance().getDatabases())
    {
        if (!dynamic_cast<const DatabaseReplicated *>(db_data.get()))
            continue;

        const bool check_access_for_db = !access->isGranted(AccessType::SHOW_DATABASES, db_name);
        if (need_to_check_access_for_databases && !check_access_for_db)
            continue;

        replicated_databases[db_name] = db_data;
    }

    auto header = storage_snapshot->metadata->getSampleBlock();
    auto reading = std::make_unique<ReadFromSystemDatabaseReplicas>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(replicated_databases), max_block_size);

    query_plan.addStep(std::move(reading));
}

}
