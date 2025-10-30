#include <Storages/ObjectStorage/DataLakes/DeltaLake/ReadFromTableChangesStep.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>

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
    DeltaLake::TableChangesPtr table_changes_,
    const Block & source_header_,
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    size_t num_streams_,
    ContextPtr context_)
    : SourceStepWithFilter(std::make_shared<const Block>(source_header_), columns_to_read_, query_info_, storage_snapshot_, context_)
    , table_changes(table_changes_)
    , source_header(source_header_)
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
    UNUSED(prewhere_info_value);
    //info = updateFormatPrewhereInfo(info, query_info.row_level_filter, prewhere_info_value);
    //query_info.prewhere_info = prewhere_info_value;
    //output_header = std::make_shared<const Block>(info.source_header);
}

void ReadFromDeltaLakeTableChangesStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto header = std::make_shared<const Block>(source_header);
    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<DeltaLakeTableChangesSource>(table_changes, header));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(header));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}

#endif
