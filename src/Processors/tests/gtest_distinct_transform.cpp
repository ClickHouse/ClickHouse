#include <gtest/gtest.h>

#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

Chunk makeChunk(const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insertValue(value);

    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), values.size());
}

/// Runs the chunks through a DistinctTransform and returns the total number of the output rows.
size_t runDistinct(SharedHeader header, Chunks chunks, bool allow_abandoning, bool skip_null_keys)
{
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
    auto transform = std::make_shared<DistinctTransform>(
        header, SizeLimits{}, /*limit_hint_=*/ 0, Names{}, allow_abandoning, skip_null_keys);

    connect(source->getPort(), transform->getInputs().front());

    auto * output_port = &transform->getOutputs().front();
    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(source));
    processors->emplace_back(std::move(transform));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    PullingPipelineExecutor executor(pipeline);

    size_t rows = 0;
    Block block;
    while (executor.pull(block))
        rows += block.rows();
    return rows;
}

}

TEST(DistinctTransformAbandon, AbandonsOnMostlyUniqueInput)
{
    const auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});

    /// Five fully unique chunks fill the observation window with a unique rate of 1, then a fully
    /// duplicate chunk follows: with abandoning allowed the transform has dropped its set by then and
    /// the duplicates pass through; with it disallowed they are removed.
    auto make_chunks = []
    {
        Chunks chunks;
        for (size_t i = 0; i < 5; ++i)
        {
            std::vector<UInt64> values(100);
            std::iota(values.begin(), values.end(), i * 100);
            chunks.push_back(makeChunk(values));
        }

        std::vector<UInt64> duplicates(100);
        std::iota(duplicates.begin(), duplicates.end(), 0);
        chunks.push_back(makeChunk(duplicates));
        return chunks;
    };

    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ true, /*skip_null_keys=*/ false), 600u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 500u);
}

TEST(DistinctTransformAbandon, KeepsDeduplicatingDuplicateHeavyInput)
{
    const auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});

    /// Every chunk repeats the same values: the unique rate stays near zero, so the transform must
    /// keep deduplicating even with abandoning allowed.
    auto make_chunks = []
    {
        Chunks chunks;
        for (size_t i = 0; i < 8; ++i)
        {
            std::vector<UInt64> values(100);
            std::iota(values.begin(), values.end(), 0);
            chunks.push_back(makeChunk(values));
        }
        return chunks;
    };

    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ true, /*skip_null_keys=*/ false), 100u);
}

TEST(DistinctTransformSkipNullKeys, DropsNullKeyRows)
{
    const auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    const auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(type, "k")});

    auto make_chunks = [&]
    {
        auto column = type->createColumn();
        column->insert(Field(UInt64(1)));
        column->insertDefault(); /// NULL
        column->insert(Field(UInt64(2)));
        column->insert(Field(UInt64(1)));
        column->insertDefault(); /// NULL

        Columns columns;
        columns.emplace_back(std::move(column));
        Chunks chunks;
        chunks.push_back(Chunk(std::move(columns), 5));
        return chunks;
    };

    /// With the skipping the NULL rows are dropped entirely; without it NULL is one distinct value.
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ true), 2u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 3u);
}

TEST(DistinctTransformSkipNullKeys, ConstNullKeyEmitsNothing)
{
    const auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    const auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(type->createColumnConst(1, Field{}), type, "c")});

    auto make_chunks = [&]
    {
        auto column = type->createColumn();
        for (size_t i = 0; i < 3; ++i)
            column->insertDefault(); /// NULL

        Columns columns;
        columns.emplace_back(std::move(column));
        Chunks chunks;
        chunks.push_back(Chunk(std::move(columns), 3));
        return chunks;
    };

    /// A constant NULL key makes every key contain a NULL: with the skipping nothing is emitted, while
    /// without it the constant-columns-only special case returns a single row.
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ true), 0u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 1u);
}
