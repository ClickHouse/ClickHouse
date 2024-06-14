#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

class ReadFromSystemTables : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemTables"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemTables(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_,
        bool need_detached_tables);

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::vector<UInt8> columns_mask;
    size_t max_block_size;
    const bool need_detached_tables;

    ColumnPtr filtered_databases_column;
    ColumnPtr filtered_tables_column;

    template <class T>
    Pipe createPipe()
    {
        return Pipe(std::make_shared<T>(
            std::move(columns_mask),
            getOutputStream().header,
            max_block_size,
            std::move(filtered_databases_column),
            std::move(filtered_tables_column),
            context));
    }
};
}
