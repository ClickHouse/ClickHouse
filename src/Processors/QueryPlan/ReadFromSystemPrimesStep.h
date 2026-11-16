#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

/// `system.primes` currently supports only a single output stream,
/// because t is not clear what the benefits or behavior of primes_mt would be.
/// So `num_streams` is intentionally not passed to CTOR.
class ReadFromSystemPrimesStep final : public SourceStepWithFilter
{
public:
    ReadFromSystemPrimesStep(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        StoragePtr storage_,
        size_t max_block_size_);

    String getName() const override { return "ReadFromSystemPrimes"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override;

private:
    Pipe makePipe();

    const Names column_names;
    StoragePtr storage;
    ExpressionActionsPtr key_expression;
    size_t max_block_size;
    std::shared_ptr<const StorageLimitsList> storage_limits;
};

}
