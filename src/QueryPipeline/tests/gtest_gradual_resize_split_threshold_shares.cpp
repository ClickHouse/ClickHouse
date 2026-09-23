#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>

using namespace DB;

/// `Pipe::resizeGradual` applies the same split into groups as `Pipe::resize` (see
/// `min_outstreams_per_resize_after_split`), and the groups do not own the same number of upstream
/// streams in general: `addSplitResizeTransform` pads the tail groups with `NullSource`s. The
/// query-level activation threshold is therefore divided among the groups in proportion to the
/// streams each group really owns, so that under balanced per-stream input every group activates
/// when about the documented total number of rows has flowed through the whole stage. Dividing by the
/// group count alone would make the smaller groups activate later. The per-group thresholds are not
/// observable from SQL, hence a pipeline-level test.

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

/// The upstream streams. Distinct from `NullSource`, which is what `addSplitResizeTransform` pads a
/// short group with, so that the two can be told apart when counting the inputs a group really owns.
class UpstreamSource final : public ISource
{
public:
    explicit UpstreamSource(SharedHeader header) : ISource(std::move(header)) {}
    String getName() const override { return "UpstreamSource"; }

protected:
    Chunk generate() override { return {}; }
};

Pipe makePipeWithStreams(size_t num_streams)
{
    auto header = makeHeader();
    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<UpstreamSource>(header));
    return Pipe::unitePipes(std::move(pipes));
}

/// The `GradualResize` processors of the pipe, in the order they were built (one per split group).
std::vector<const GradualResizeProcessor *> gradualResizeProcessors(const Pipe & pipe)
{
    std::vector<const GradualResizeProcessor *> result;
    for (const auto & processor : pipe.getProcessors())
        if (const auto * resize = typeid_cast<const GradualResizeProcessor *>(processor.get()))
            result.push_back(resize);
    return result;
}

size_t realInputs(const GradualResizeProcessor & resize)
{
    size_t result = 0;
    for (const auto & input : resize.getInputs())
        if (!typeid_cast<const NullSource *>(&input.getOutputPort().getProcessor()))
            ++result;
    return result;
}

}

TEST(GradualResizeSplitThresholdShares, UnevenGroupsGetProportionalShares)
{
    /// 14 upstream streams, 12 aggregating streams and at least 4 outputs per group: 3 groups, which
    /// own 5, 5 and 4 of the upstream streams (the last input of the third group is a `NullSource`).
    auto pipe = makePipeWithStreams(14);
    pipe.resizeGradual(12, /* min_rows_per_output = */ 3000, /* min_bytes_per_output = */ 1400, /* min_outstreams_per_resize_after_split = */ 4);

    auto resizes = gradualResizeProcessors(pipe);
    ASSERT_EQ(resizes.size(), 3u);

    std::vector<size_t> inputs;
    std::vector<size_t> rows_thresholds;
    std::vector<size_t> bytes_thresholds;
    for (const auto * resize : resizes)
    {
        EXPECT_EQ(resize->getInputs().size(), 5u);
        EXPECT_EQ(resize->getOutputs().size(), 4u);
        inputs.push_back(realInputs(*resize));
        rows_thresholds.push_back(resize->getMinRowsThreshold());
        bytes_thresholds.push_back(resize->getMinBytesThreshold());
    }

    EXPECT_EQ(inputs, (std::vector<size_t>{5, 5, 4}));
    /// ceil(3000 * 5 / 14) = 1072 and ceil(3000 * 4 / 14) = 858; a plain `3000 / 3 = 1000` for every
    /// group would make the 4-stream group activate only after about 3500 rows in total.
    EXPECT_EQ(rows_thresholds, (std::vector<size_t>{1072, 1072, 858}));
    /// ceil(1400 * 5 / 14) = 500 and ceil(1400 * 4 / 14) = 400.
    EXPECT_EQ(bytes_thresholds, (std::vector<size_t>{500, 500, 400}));
}

TEST(GradualResizeSplitThresholdShares, EqualGroupsSplitEvenly)
{
    /// 16 upstream streams into 16 aggregating streams, 4 per group: 4 equal groups, so every group
    /// gets exactly a quarter, the same as the former division by the group count.
    auto pipe = makePipeWithStreams(16);
    pipe.resizeGradual(16, /* min_rows_per_output = */ 400000, /* min_bytes_per_output = */ 0, /* min_outstreams_per_resize_after_split = */ 4);

    auto resizes = gradualResizeProcessors(pipe);
    ASSERT_EQ(resizes.size(), 4u);
    for (const auto * resize : resizes)
    {
        EXPECT_EQ(realInputs(*resize), 4u);
        EXPECT_EQ(resize->getMinRowsThreshold(), 100000u);
        /// A disabled byte threshold stays disabled.
        EXPECT_EQ(resize->getMinBytesThreshold(), 0u);
    }
}

TEST(GradualResizeSplitThresholdShares, SmallThresholdStaysEnabledInEveryGroup)
{
    /// A threshold below the number of streams must not round down to 0 for any group, since 0
    /// means "this threshold is disabled".
    auto pipe = makePipeWithStreams(14);
    pipe.resizeGradual(12, /* min_rows_per_output = */ 1, /* min_bytes_per_output = */ 0, /* min_outstreams_per_resize_after_split = */ 4);

    auto resizes = gradualResizeProcessors(pipe);
    ASSERT_EQ(resizes.size(), 3u);
    for (const auto * resize : resizes)
        EXPECT_EQ(resize->getMinRowsThreshold(), 1u);
}

TEST(GradualResizeSplitThresholdShares, NoSplitKeepsTheWholeThreshold)
{
    auto pipe = makePipeWithStreams(14);
    pipe.resizeGradual(12, /* min_rows_per_output = */ 3000, /* min_bytes_per_output = */ 1400, /* min_outstreams_per_resize_after_split = */ 0);

    auto resizes = gradualResizeProcessors(pipe);
    ASSERT_EQ(resizes.size(), 1u);
    EXPECT_EQ(resizes.front()->getMinRowsThreshold(), 3000u);
    EXPECT_EQ(resizes.front()->getMinBytesThreshold(), 1400u);
}
