#include "StorageSystemDetachedTables.h"

#include <Access/ContextAccess.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace
{

class DetachedTablesBlockSource : public ISource
{
public:
    DetachedTablesBlockSource(
        std::vector<UInt8> columns_mask_,
        Block header_,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr detached_tables_,
        ContextPtr context_)
        : ISource(std::move(header_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(Context::createCopy(context_))
    {
        size_t size = detached_tables_->size();
        detached_tables.reserve(size);
        for (size_t idx = 0; idx < size; ++idx)
        {
            detached_tables.insert(detached_tables_->getDataAt(idx).toString());
        }
    }

    String getName() const override { return "DetachedTables"; }

protected:
    Chunk generate() override
    {
        if (done)
            return {};

        MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context->getAccess();
        const bool need_to_check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t database_idx = 0;
        size_t rows_count = 0;
        for (; database_idx < databases->size() && rows_count < max_block_size; ++database_idx)
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

        if (databases->size() == database_idx && (!detached_tables_it || !detached_tables_it->isValid()))
        {
            done = true;
        }
        const UInt64 num_rows = result_columns.at(0)->size();
        return Chunk(std::move(result_columns), num_rows);
    }

private:
    const std::vector<UInt8> columns_mask;
    const UInt64 max_block_size;
    const ColumnPtr databases;
    NameSet detached_tables;
    DatabaseDetachedTablesSnapshotIteratorPtr detached_tables_it;
    ContextPtr context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;

    void fillResultColumnsByDetachedTableIterator(MutableColumns & result_columns) const
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
};

}

class ReadFromSystemDetachedTables : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemDetachedTables"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemDetachedTables(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_);

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::vector<UInt8> columns_mask;
    size_t max_block_size;

    ColumnPtr filtered_databases_column;
    ColumnPtr filtered_tables_column;
};

StorageSystemDetachedTables::StorageSystemDetachedTables(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    auto description = ColumnsDescription{
        ColumnDescription{"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        ColumnDescription{"table", std::make_shared<DataTypeString>(), "Table name."},
        ColumnDescription{"uuid", std::make_shared<DataTypeUUID>(), "Table uuid (Atomic database)."},
        ColumnDescription{"metadata_path", std::make_shared<DataTypeString>(), "Path to the table metadata in the file system."},
        ColumnDescription{"is_permanently", std::make_shared<DataTypeUInt8>(), "Table was detached permanently."},
    };

    storage_metadata.setColumns(std::move(description));

    setInMemoryMetadata(storage_metadata);
}

void StorageSystemDetachedTables::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    auto sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, res_block] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto reading = std::make_unique<ReadFromSystemDetachedTables>(
        column_names, query_info, storage_snapshot, context, std::move(res_block), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

ReadFromSystemDetachedTables::ReadFromSystemDetachedTables(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    Block sample_block,
    std::vector<UInt8> columns_mask_,
    size_t max_block_size_)
    : SourceStepWithFilter(std::move(sample_block), column_names_, query_info_, storage_snapshot_, context_)
    , columns_mask(std::move(columns_mask_))
    , max_block_size(max_block_size_)
{
}

void ReadFromSystemDetachedTables::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    filtered_databases_column = detail::getFilteredDatabases(predicate, context);
    filtered_tables_column = detail::getFilteredTables(predicate, filtered_databases_column, context, true);
}

void ReadFromSystemDetachedTables::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = Pipe(std::make_shared<DetachedTablesBlockSource>(
        std::move(columns_mask),
        getOutputHeader(),
        max_block_size,
        std::move(filtered_databases_column),
        std::move(filtered_tables_column),
        context));
    pipeline.init(std::move(pipe));
}
}
