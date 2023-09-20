#include <gtest/gtest.h>


#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <random>

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

UInt64 getAndPrintRandomSeed()
{
    UInt64 seed = randomSeed();
    std::cerr << "TEST_RANDOM_SEED: " << seed << std::endl;
    return seed;
}

static UInt64 TEST_RANDOM_SEED = getAndPrintRandomSeed();

static pcg64 rng(TEST_RANDOM_SEED);


QueryPipeline buildJoinPipeline(
    std::shared_ptr<ISource> left_source,
    std::shared_ptr<ISource> right_source,
    size_t key_length = 1,
    JoinKind kind = JoinKind::Inner,
    JoinStrictness strictness = JoinStrictness::All,
    ASOFJoinInequality asof_inequality = ASOFJoinInequality::None)
{
    Blocks inputs;
    inputs.emplace_back(left_source->getPort().getHeader());
    inputs.emplace_back(right_source->getPort().getHeader());

    Block out_header;
    for (const auto & input : inputs)
    {
        for (ColumnWithTypeAndName column : input)
        {
            if (&input == &inputs.front())
                column.name = "t1." + column.name;
            else
                column.name = "t2." + column.name;
            out_header.insert(column);
        }
    }

    TableJoin::JoinOnClause on_clause;
    for (size_t i = 0; i < key_length; ++i)
    {
        on_clause.key_names_left.emplace_back(inputs[0].getByPosition(i).name);
        on_clause.key_names_right.emplace_back(inputs[1].getByPosition(i).name);
    }

    auto joining = std::make_shared<MergeJoinTransform>(
        kind,
        strictness,
        on_clause,
        inputs, out_header, /* max_block_size = */ 0);

    if (asof_inequality != ASOFJoinInequality::None)
        joining->setAsofInequality(asof_inequality);

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


std::shared_ptr<ISource> oneColumnSource(const std::vector<std::vector<UInt64>> & values)
{
    Block header = { ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "x") };
    Chunks chunks;
    for (const auto & chunk_values : values)
    {
        auto column = ColumnUInt64::create();
        for (auto n : chunk_values)
            column->insertValue(n);
        chunks.emplace_back(Chunk(Columns{std::move(column)}, chunk_values.size()));
    }
    return std::make_shared<SourceFromChunks>(header, std::move(chunks));
}


TEST(FullSortingJoin, Simple)
try
{
    auto left_source = oneColumnSource({ {1, 2, 3, 4, 5} });
    auto right_source = oneColumnSource({ {1}, {2}, {3}, {4}, {5} });

    auto pipeline = buildJoinPipeline(left_source, right_source);
    PullingPipelineExecutor executor(pipeline);

    Block block;

    size_t total_result_rows = 0;
    while (executor.pull(block))
        total_result_rows += block.rows();

    ASSERT_EQ(total_result_rows, 5);
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}

class SourceChunksBuilder
{
public:
    explicit SourceChunksBuilder(const Block & header_)
        : header(header_)
    {
        current_chunk = header.cloneEmptyColumns();
        chassert(!current_chunk.empty());
    }

    SourceChunksBuilder & addRow(const std::vector<Field> & row)
    {
        chassert(row.size() == current_chunk.size());
        for (size_t i = 0; i < current_chunk.size(); ++i)
            current_chunk[i]->insert(row[i]);
        return *this;
    }

    SourceChunksBuilder & addChunk()
    {
        if (current_chunk.front()->empty())
            return *this;

        size_t rows = current_chunk.front()->size();
        chunks.emplace_back(std::move(current_chunk), rows);
        current_chunk = header.cloneEmptyColumns();
        return *this;
    }

    std::shared_ptr<ISource> build()
    {
        addChunk();
        return std::make_shared<SourceFromChunks>(header, std::move(chunks));
    }

private:
    Block header;
    Chunks chunks;
    MutableColumns current_chunk;
};


std::vector<std::vector<Field>> getValuesFromBlock(const Block & block, const Names & names)
{
    std::vector<std::vector<Field>> result;
    for (size_t i = 0; i < block.rows(); ++i)
    {
        auto & row = result.emplace_back();
        for (const auto & name : names)
            block.getByName(name).column->get(i, row.emplace_back());
    }
    return result;
}


Block executePipeline(QueryPipeline & pipeline)
{
    PullingPipelineExecutor executor(pipeline);

    Blocks result_blocks;
    while (true)
    {
        Block block;
        bool is_ok = executor.pull(block);
        if (!is_ok)
            break;
        result_blocks.emplace_back(std::move(block));
    }

    return concatenateBlocks(result_blocks);
}

TEST(FullSortingJoin, Asof)
try
{
    auto left_source = SourceChunksBuilder({
        {std::make_shared<DataTypeString>(), "key"},
        {std::make_shared<DataTypeUInt64>(), "t"},
    })
        .addRow({"AMZN", 3})
        .addRow({"AMZN", 4})
        .addRow({"AMZN", 6})
        .build();

    auto right_source = SourceChunksBuilder({
        {std::make_shared<DataTypeString>(), "key"},
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeUInt64>(), "value"},
    })
        .addRow({"AAPL", 1, 97})
        .addChunk()
        .addRow({"AAPL", 2, 98})
        .addRow({"AAPL", 3, 99})
        .addRow({"AMZN", 1, 100})
        .addRow({"AMZN", 2, 110})
        .addChunk()
        .addRow({"AMZN", 4, 130})
        .addRow({"AMZN", 5, 140})
        .build();

    auto pipeline = buildJoinPipeline(
        left_source, right_source, /* key_length = */ 2,
        JoinKind::Inner, JoinStrictness::Asof, ASOFJoinInequality::LessOrEquals);

    Block result_block = executePipeline(pipeline);
    auto values = getValuesFromBlock(result_block, {"t1.key", "t1.t", "t2.t", "t2.value"});

    ASSERT_EQ(values, (std::vector<std::vector<Field>>{
        {"AMZN", 3u, 4u, 130u},
        {"AMZN", 4u, 4u, 130u},
    }));
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}


TEST(FullSortingJoin, AsofOnlyColumn)
try
{
    auto left_source = oneColumnSource({ {3}, {3, 3, 3}, {3, 5, 5, 6}, {9, 9}, {10, 20} });

    UInt64 p = std::uniform_int_distribution<>(0, 2)(rng);

    SourceChunksBuilder right_source_builder({
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeUInt64>(), "value"},
    });

    double break_prob = p == 0 ? 0.0 : (p == 1 ? 0.5 : 1.0);
    std::uniform_real_distribution<> prob_dis(0.0, 1.0);
    for (const auto & row : std::vector<std::vector<Field>>{ {1, 101}, {2, 102}, {4, 104}, {5, 105}, {11, 111}, {15, 115} })
    {
        right_source_builder.addRow(row);
        if (prob_dis(rng) < break_prob)
            right_source_builder.addChunk();
    }
    auto right_source = right_source_builder.build();

    auto pipeline = buildJoinPipeline(
        left_source, right_source, /* key_length = */ 1,
        JoinKind::Inner, JoinStrictness::Asof, ASOFJoinInequality::LessOrEquals);

    Block result_block = executePipeline(pipeline);

    ASSERT_EQ(
        assert_cast<const ColumnUInt64 *>(result_block.getByName("t1.x").column.get())->getData(),
        (ColumnUInt64::Container{3, 3, 3, 3, 3, 5, 5, 6, 9, 9, 10})
    );

    ASSERT_EQ(
        assert_cast<const ColumnUInt64 *>(result_block.getByName("t2.t").column.get())->getData(),
        (ColumnUInt64::Container{4, 4, 4, 4, 4, 5, 5, 11, 11, 11, 11})
    );

    ASSERT_EQ(
        assert_cast<const ColumnUInt64 *>(result_block.getByName("t2.value").column.get())->getData(),
        (ColumnUInt64::Container{104, 104, 104, 104, 104, 105, 105, 111, 111, 111, 111})
    );
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}
