#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace
{

struct TestChunkInfo : ChunkInfoCloneable<TestChunkInfo>
{
};

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "k")});
}

Chunk makeChunk(UInt64 value)
{
    auto column = ColumnUInt64::create();
    column->insertValue(value);
    column->insertValue(value + 1);
    return Chunk(Columns{std::move(column)}, 2);
}

struct ConnectedLimit
{
    SharedHeader header = makeHeader();
    DistinctLimitTransform transform;
    OutputPorts upstream;
    InputPorts downstream;
    IProcessor::UpdatedInputPorts inputs;
    IProcessor::UpdatedOutputPorts outputs;

    explicit ConnectedLimit(size_t streams, const SizeLimits & size_limits = {})
        : transform(header, size_limits, streams)
        , upstream(streams, header)
        , downstream(streams, header)
    {
        auto input = transform.getInputs().begin();
        auto output = transform.getOutputs().begin();
        auto sink = downstream.begin();
        for (auto & source : upstream)
        {
            connect(source, *input);
            connect(*output, *sink++);
            inputs.push_back(&*input++);
            outputs.push_back(&*output++);
        }
    }

    IProcessor::Status prepare() { return transform.prepare(inputs, outputs); }
};

}

TEST(DistinctLimitTransform, KeepsStreamsSeparateAndHonorsDemand)
{
    ConnectedLimit limit(4);
    EXPECT_EQ(limit.prepare(), IProcessor::Status::PortFull);
    for (const auto & source : limit.upstream)
        EXPECT_FALSE(source.canPush());

    auto source = limit.upstream.begin();
    size_t stream = 0;
    for (auto & sink : limit.downstream)
    {
        sink.setNeeded();
        limit.prepare();
        ASSERT_TRUE(source->canPush());
        auto chunk = makeChunk(stream * 10);
        DistinctTransform distinct(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
        static_cast<ISimpleTransform &>(distinct).transform(chunk);
        chunk.getChunkInfos().add(std::make_shared<TestChunkInfo>());
        source->push(std::move(chunk));
        limit.transform.prepare({limit.inputs[stream]}, {});
        ASSERT_TRUE(sink.hasData());
        const auto result = sink.pull();
        EXPECT_EQ(assert_cast<const ColumnUInt64 &>(*result.getColumns().front()).getElement(0), stream * 10);
        EXPECT_NE(result.getChunkInfos().get<TestChunkInfo>(), nullptr);
        EXPECT_EQ(result.getChunkInfos().size(), 1);
        sink.setNotNeeded();
        limit.prepare();
        EXPECT_FALSE(source->canPush());
        ++source;
        ++stream;
    }

    limit.downstream.front().close();
    limit.prepare();
    EXPECT_TRUE(limit.upstream.front().isFinished());
    EXPECT_FALSE(limit.upstream.back().isFinished());

    for (auto & sink : limit.downstream)
        sink.setNeeded();
    for (auto & port : limit.upstream)
        port.finish();
    EXPECT_EQ(limit.prepare(), IProcessor::Status::Finished);
    for (const auto & sink : limit.downstream)
        EXPECT_TRUE(sink.isFinished());
}

TEST(DistinctLimitTransform, GlobalBreakStopsIdlePartitionsAfterEmittingTheLastChunk)
{
    for (size_t streams : {1, 4})
    {
        SCOPED_TRACE(streams);
        ConnectedLimit limit(streams, SizeLimits(1, 0, OverflowMode::BREAK));
        for (auto & sink : limit.downstream)
            sink.setNeeded();
        EXPECT_EQ(limit.prepare(), IProcessor::Status::NeedData);

        DistinctTransform distinct(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
        auto chunk = makeChunk(10);
        static_cast<ISimpleTransform &>(distinct).transform(chunk);
        chunk.getChunkInfos().add(std::make_shared<TestChunkInfo>());
        limit.upstream.back().push(std::move(chunk));

        const auto status = streams == 1 ? limit.transform.prepare() : limit.transform.prepare({limit.inputs.back()}, {});
        EXPECT_EQ(status, IProcessor::Status::Finished);
        ASSERT_TRUE(limit.downstream.back().hasData());
        const auto result = limit.downstream.back().pull();
        EXPECT_EQ(result.getNumRows(), 2);
        EXPECT_EQ(assert_cast<const ColumnUInt64 &>(*result.getColumns().front()).getElement(0), 10);
        EXPECT_NE(result.getChunkInfos().get<TestChunkInfo>(), nullptr);
        for (const auto & source : limit.upstream)
            EXPECT_TRUE(source.isFinished());
        for (const auto & sink : limit.downstream)
            EXPECT_TRUE(sink.isFinished());
    }
}

TEST(DistinctLimitTransform, ForwardsExceptionsThroughTheirOriginalStream)
{
    ConnectedLimit limit(2);
    limit.downstream.back().setNeeded();
    limit.prepare();
    const auto exception = std::make_exception_ptr(std::runtime_error("test exception"));
    limit.upstream.back().pushException(exception);
    limit.transform.prepare({limit.inputs.back()}, {});
    ASSERT_TRUE(limit.downstream.back().hasData());
    EXPECT_EQ(limit.downstream.back().pullData().exception, exception);
    EXPECT_FALSE(limit.downstream.front().hasData());
}

TEST(DistinctLimitTransform, ClosedOutputDoesNotPreventGlobalBreakOnOtherStreams)
{
    ConnectedLimit limit(2, SizeLimits(3, 0, OverflowMode::BREAK));
    for (auto & sink : limit.downstream)
        sink.setNeeded();
    limit.prepare();

    DistinctTransform first(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
    DistinctTransform second(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
    auto discarded = makeChunk(0);
    static_cast<ISimpleTransform &>(first).transform(discarded);
    limit.upstream.front().push(std::move(discarded));
    limit.downstream.front().close();
    limit.prepare();
    EXPECT_TRUE(limit.upstream.front().isFinished());
    EXPECT_FALSE(limit.upstream.back().isFinished());

    for (UInt64 value : {10, 20})
    {
        limit.downstream.back().setNeeded();
        limit.prepare();
        auto chunk = makeChunk(value);
        static_cast<ISimpleTransform &>(second).transform(chunk);
        limit.upstream.back().push(std::move(chunk));
        const auto status = limit.transform.prepare({limit.inputs.back()}, {});
        EXPECT_EQ(status, value == 20 ? IProcessor::Status::Finished : IProcessor::Status::PortFull);
        ASSERT_TRUE(limit.downstream.back().hasData());
        EXPECT_EQ(limit.downstream.back().pull().getNumRows(), 2);
    }
    EXPECT_TRUE(limit.upstream.back().isFinished());
}

TEST(DistinctLimitTransform, ThrowLimitCountsKeysAcrossStreams)
{
    ConnectedLimit limit(2, SizeLimits(3, 0, OverflowMode::THROW));
    for (auto & sink : limit.downstream)
        sink.setNeeded();
    limit.prepare();

    DistinctTransform first(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
    DistinctTransform second(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
    auto first_chunk = makeChunk(0);
    auto second_chunk = makeChunk(10);
    static_cast<ISimpleTransform &>(first).transform(first_chunk);
    static_cast<ISimpleTransform &>(second).transform(second_chunk);
    limit.upstream.front().push(std::move(first_chunk));
    limit.transform.prepare({limit.inputs.front()}, {});
    ASSERT_TRUE(limit.downstream.front().hasData());
    EXPECT_EQ(limit.downstream.front().pull().getNumRows(), 2);

    limit.upstream.back().push(std::move(second_chunk));
    EXPECT_THROW(limit.transform.prepare({limit.inputs.back()}, {}), Exception);
}

TEST(DistinctLimitTransform, PreservesEmptyChunkMetadata)
{
    ConnectedLimit limit(1, SizeLimits(1, 0, OverflowMode::BREAK));
    limit.downstream.front().setNeeded();
    limit.prepare();
    Chunk chunk(Columns{ColumnUInt64::create()}, 0);
    chunk.getChunkInfos().add(std::make_shared<TestChunkInfo>());
    limit.upstream.front().push(std::move(chunk));
    EXPECT_EQ(limit.transform.prepare(), IProcessor::Status::PortFull);
    ASSERT_TRUE(limit.downstream.front().hasData());
    const auto result = limit.downstream.front().pull();
    EXPECT_EQ(result.getNumRows(), 0);
    EXPECT_NE(result.getChunkInfos().get<TestChunkInfo>(), nullptr);
}

TEST(DistinctLimitTransform, CountsConstantKeysWithoutSetAllocations)
{
    ConnectedLimit limit(1, SizeLimits(1, 0, OverflowMode::BREAK));
    const ColumnPtr constant = ColumnConst::create(ColumnUInt64::create(1, UInt64(7)), 4);
    const auto header
        = std::make_shared<const Block>(Block{ColumnWithTypeAndName(constant->cloneEmpty(), std::make_shared<DataTypeUInt64>(), "k")});
    DistinctTransform distinct(header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
    Chunk chunk(Columns{constant}, 4);
    static_cast<ISimpleTransform &>(distinct).transform(chunk);
    limit.downstream.front().setNeeded();
    limit.prepare();
    limit.upstream.front().push(std::move(chunk));
    EXPECT_EQ(limit.transform.prepare(), IProcessor::Status::Finished);
    ASSERT_TRUE(limit.downstream.front().hasData());
    EXPECT_EQ(limit.downstream.front().pull().getNumRows(), 1);
}

TEST(DistinctLimitTransform, GlobalBreakStopsWithinUpdatedBatch)
{
    for (bool notify_inputs : {false, true})
    {
        SCOPED_TRACE(notify_inputs);
        ConnectedLimit limit(3, SizeLimits(1, 0, OverflowMode::BREAK));
        for (auto & sink : limit.downstream)
            sink.setNeeded();
        limit.prepare();

        UInt64 value = 0;
        for (auto & source : limit.upstream)
        {
            DistinctTransform distinct(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
            auto chunk = makeChunk(value);
            static_cast<ISimpleTransform &>(distinct).transform(chunk);
            source.push(std::move(chunk));
            value += 10;
        }

        const auto updated_inputs = notify_inputs ? limit.inputs : IProcessor::UpdatedInputPorts{};
        EXPECT_EQ(limit.transform.prepare(updated_inputs, limit.outputs), IProcessor::Status::Finished);
        ASSERT_TRUE(limit.downstream.front().hasData());
        EXPECT_EQ(limit.downstream.front().pull().getNumRows(), 2);
        for (const auto & sink : limit.downstream)
        {
            EXPECT_FALSE(sink.hasData());
            EXPECT_TRUE(sink.isFinished());
        }
        for (const auto & source : limit.upstream)
            EXPECT_TRUE(source.isFinished());
    }
}

TEST(DistinctLimitTransform, FinishedPairIsCountedOnceAcrossRepeatedUpdates)
{
    ConnectedLimit limit(2);
    for (auto & sink : limit.downstream)
        sink.setNeeded();
    limit.prepare();
    limit.downstream.front().close();

    for (size_t update = 0; update < 3; ++update)
    {
        EXPECT_EQ(limit.transform.prepare({limit.inputs.front()}, {limit.outputs.front()}), IProcessor::Status::NeedData);
        EXPECT_TRUE(limit.upstream.front().isFinished());
        EXPECT_FALSE(limit.upstream.back().isFinished());
    }

    limit.upstream.back().finish();
    EXPECT_EQ(limit.transform.prepare({limit.inputs.back()}, {}), IProcessor::Status::Finished);
    EXPECT_TRUE(limit.downstream.back().isFinished());
}

TEST(DistinctLimitTransform, ByteLimitUsesRetainedSetAllocations)
{
    for (OverflowMode mode : {OverflowMode::BREAK, OverflowMode::THROW})
    {
        SCOPED_TRACE(static_cast<int>(mode));
        ConnectedLimit limit(2, SizeLimits(0, 1, mode));
        for (auto & sink : limit.downstream)
            sink.setNeeded();
        limit.prepare();

        DistinctTransform distinct(limit.header, {}, 0, Names{"k"}, false, false, /*max_bytes_before_pass_through_=*/ 0, /*report_set_size_=*/ true);
        auto chunk = makeChunk(10);
        static_cast<ISimpleTransform &>(distinct).transform(chunk);
        limit.upstream.back().push(std::move(chunk));

        if (mode == OverflowMode::THROW)
        {
            EXPECT_THROW(limit.transform.prepare({limit.inputs.back()}, {}), Exception);
            EXPECT_FALSE(limit.downstream.back().hasData());
        }
        else
        {
            EXPECT_EQ(limit.transform.prepare({limit.inputs.back()}, {}), IProcessor::Status::Finished);
            ASSERT_TRUE(limit.downstream.back().hasData());
            EXPECT_EQ(limit.downstream.back().pull().getNumRows(), 2);
            for (const auto & source : limit.upstream)
                EXPECT_TRUE(source.isFinished());
            for (const auto & sink : limit.downstream)
                EXPECT_TRUE(sink.isFinished());
        }
    }
}
