#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>

namespace DB
{
class ReadFromDeltaLakeTableChangesStep : public SourceStepWithFilter
{
public:
    static constexpr auto STEP_NAME = "ReadFromDeltaLakeTableChangesStorage";

    ReadFromDeltaLakeTableChangesStep(
        DeltaLake::TableChangesPtr table_changes_,
        const Block & source_header_,
        const Names & columns_to_read_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        size_t num_streams_,
        ContextPtr context_);

    std::string getName() const override { return STEP_NAME; }

    QueryPlanStepPtr clone() const override;

    StorageMetadataPtr getStorageMetadata() const { return storage_snapshot->metadata; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    DeltaLake::TableChangesPtr table_changes;
    Block source_header;
    size_t num_streams;
};
}

#endif
