#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>

namespace DB
{

/**
 * A read step to read from DeltaLake CDF.
 */
class ReadFromDeltaLakeTableChangesStep : public SourceStepWithFilter
{
public:
    static constexpr auto STEP_NAME = "ReadFromDeltaLakeTableChangesStorage";

    ReadFromDeltaLakeTableChangesStep(
        const DeltaLake::TableChangesPtr & table_changes_,
        const Block & header_,
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
    const DeltaLake::TableChangesPtr table_changes;
    const Block header;
    const size_t num_streams;
};
}

#endif
