#include <gtest/gtest.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/TableJoin.h>

#include <Processors/Transforms/MergeJoinTransform.h>


using namespace DB;


QueryPipeline buildJoinPipeline(std::shared_ptr<ISource> left_source, std::shared_ptr<ISource> right_source)
{
    Blocks inputs;
    inputs.emplace_back(left_source->getPort().getHeader());
    inputs.emplace_back(right_source->getPort().getHeader());
    Block out_header = {
        ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "t1.x"),
        ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "t2.x"),
    };

    TableJoin::JoinOnClause on_clause;
    on_clause.key_names_left = {"x"};
    on_clause.key_names_right = {"x"};
    auto joining = std::make_shared<MergeJoinTransform>(
        JoinKind::Inner,
        JoinStrictness::All,
        on_clause,
        inputs, out_header, /* max_block_size = */ 0);

    chassert(joining->getInputs().size() == 2);

    connect(left_source->getPort(), joining->getInputs().front());
    connect(right_source->getPort(), joining->getInputs().back());

    auto * output_port = &joining->getOutputPort();

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(left_source));
    processors->emplace_back(std::move(right_source));
    processors->emplace_back(std::move(joining));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    return pipeline;
}


std::shared_ptr<ISource> createSourceWithSingleValue(size_t rows_per_chunk, size_t total_chunks)
{
    Block header = {
        ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")
    };

    Chunks chunks;

    for (size_t i = 0; i < total_chunks; ++i)
    {
        auto col = ColumnUInt64::create(rows_per_chunk, 1);
        chunks.emplace_back(Columns{std::move(col)}, rows_per_chunk);
    }

    return std::make_shared<SourceFromChunks>(std::move(header), std::move(chunks));
}

TEST(FullSortingJoin, Simple)
try
{
    auto left_source = createSourceWithSingleValue(3, 10);
    auto right_source = createSourceWithSingleValue(2, 15);

    auto pipeline = buildJoinPipeline(left_source, right_source);
    PullingPipelineExecutor executor(pipeline);

    Block block;

    size_t total_result_rows = 0;
    while (executor.pull(block))
    {
        total_result_rows += block.rows();
    }
    ASSERT_EQ(total_result_rows, 3 * 10 * 2 * 15);
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}
