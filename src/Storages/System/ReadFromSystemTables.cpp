#include "ReadFromSystemTables.h"

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/DatabaseCatalog.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/System/TablesBlockSource.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/range/adaptor/map.hpp>

namespace DB
{

namespace
{

ColumnPtr getFilteredDatabases(const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & database_name : databases | boost::adaptors::map_keys)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.tables

        column->insert(database_name);
    }

    Block block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database")};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

ColumnPtr getFilteredTables(
    const ActionsDAG::Node * predicate, const ColumnPtr & filtered_databases_column, ContextPtr context, const bool need_detached_tables)
{
    Block sample{
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "engine")};

    MutableColumnPtr database_column = ColumnString::create();
    MutableColumnPtr engine_column;

    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(predicate, &sample);
    if (dag)
    {
        bool filter_by_engine = false;
        for (const auto * input : dag->getInputs())
            if (input->result_name == "engine")
                filter_by_engine = true;

        if (filter_by_engine)
            engine_column = ColumnString::create();
    }

    for (size_t database_idx = 0; database_idx < filtered_databases_column->size(); ++database_idx)
    {
        const auto & database_name = filtered_databases_column->getDataAt(database_idx).toString();
        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
            continue;

        if (need_detached_tables)
        {
            auto table_it = database->getDetachedTablesIterator(context, {}, false);
            for (; table_it->isValid(); table_it->next())
            {
                database_column->insert(table_it->table());
            }
        }
        else
        {
            auto table_it = database->getTablesIterator(context);
            for (; table_it->isValid(); table_it->next())
            {
                database_column->insert(table_it->name());
                if (engine_column)
                    engine_column->insert(table_it->table()->getName());
            }
        }
    }

    Block block{ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "name")};
    if (engine_column)
        block.insert(ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine"));

    if (dag)
        VirtualColumnUtils::filterBlockWithDAG(dag, block, context);

    return block.getByPosition(0).column;
}

}

ReadFromSystemTables::ReadFromSystemTables(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    Block sample_block,
    std::vector<UInt8> columns_mask_,
    size_t max_block_size_,
    const bool need_detached_tables_)
    : SourceStepWithFilter(DataStream{.header = std::move(sample_block)}, column_names_, query_info_, storage_snapshot_, context_)
    , columns_mask(std::move(columns_mask_))
    , max_block_size(max_block_size_)
    , need_detached_tables(need_detached_tables_)
{
}

void ReadFromSystemTables::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    filtered_databases_column = getFilteredDatabases(predicate, context);
    filtered_tables_column = getFilteredTables(predicate, filtered_databases_column, context, need_detached_tables);
}

void ReadFromSystemTables::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (need_detached_tables)
    {
        pipeline.init(createPipe<DetachedTablesBlockSource>());
    }
    else
    {
        pipeline.init(createPipe<TablesBlockSource>());
    }
}
}
