#include <Storages/ObjectStorage/DataLakes/DeltaLake/ReadFromTableChangesStep.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace
{

class DeltaLakeTableChangesSource : public ISource
{
public:
    DeltaLakeTableChangesSource(
        DeltaLake::TableChangesPtr table_changes_,
        std::shared_ptr<const Block> source_header_)
        : ISource(source_header_, false)
        , table_changes(table_changes_)
    {
    }

    String getName() const override { return "DeltaLakeTableChangesSource"; }

    Chunk generate() override
    {
        return table_changes->next();
    }

    void onFinish() override { }

private:
    DeltaLake::TableChangesPtr table_changes;
};

}

ReadFromDeltaLakeTableChangesStep::ReadFromDeltaLakeTableChangesStep(
    const DeltaLake::TableChangesPtr & table_changes_,
    const Block & header_,
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    size_t num_streams_,
    ContextPtr context_)
    : SourceStepWithFilter(
        std::make_shared<const Block>(header_),
        columns_to_read_,
        query_info_,
        storage_snapshot_,
        context_)
    , table_changes(table_changes_)
    , header(header_)
    , num_streams(num_streams_)
{
}

QueryPlanStepPtr ReadFromDeltaLakeTableChangesStep::clone() const
{
    return std::make_unique<ReadFromDeltaLakeTableChangesStep>(*this);
}

void ReadFromDeltaLakeTableChangesStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
}

void ReadFromDeltaLakeTableChangesStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    query_info.prewhere_info = prewhere_info_value;
    output_header = std::make_shared<const Block>(header);
}

void ReadFromDeltaLakeTableChangesStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (filter_actions_dag)
        table_changes->setFilter(filter_actions_dag.get());

    auto source_header = std::make_shared<const Block>(header);
    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<DeltaLakeTableChangesSource>(table_changes, source_header));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(source_header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}

#endif
