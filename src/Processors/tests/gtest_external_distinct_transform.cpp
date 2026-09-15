#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <thread>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Processors/Merges/DistinctSortedTransform.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/ExternalDistinctTransform.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/scope_guard.h>

using namespace DB;

namespace
{

Chunk makeChunk(std::initializer_list<UInt64> values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insertValue(value);
    return Chunk(Columns{std::move(column)}, values.size());
}

struct ConnectedDistinct
{
    SharedHeader header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});
    OutputPort upstream{header};
    InputPort downstream{header};
    ExternalDistinctTransform transform;
    Processors processors;

    explicit ConnectedDistinct(TemporaryDataOnDiskScopePtr tmp_data, UInt64 limit_hint = 0)
        : transform(header, SizeLimits{}, limit_hint, Names{}, /*max_bytes_before_external_distinct_=*/ 1,
            std::move(tmp_data), /*min_free_disk_space_=*/ 0, /*max_block_size_rows_=*/ 2, /*preserve_input_order_=*/ false)
    {
        connect(upstream, transform.getInputs().front());
        connect(transform.getOutputs().front(), downstream);
        downstream.setNeeded();
    }

    struct RunProcessors
    {
        BufferingToFileSink * sink = nullptr;
        BufferingFromFileSource * source = nullptr;
    };

    RunProcessors attachRun()
    {
        auto update = transform.updatePipeline();
        RunProcessors run;
        for (const auto & processor : update.to_add)
        {
            if (auto * sink = typeid_cast<BufferingToFileSink *>(processor.get()))
                run.sink = sink;
            else if (auto * source = typeid_cast<BufferingFromFileSource *>(processor.get()))
                run.source = source;
        }
        processors.splice(processors.end(), update.to_add);
        return run;
    }
};

class ExternalDistinctTransformTest : public testing::Test
{
protected:
    void SetUp() override
    {
        disk = createDisk("external_distinct");
        tmp_data = std::make_shared<TemporaryDataOnDiskScope>(
            TemporaryDataOnDiskSettings{}, std::make_shared<SingleDiskVolume>("temporary", disk));
    }

    void TearDown() override
    {
        tmp_data.reset();
        destroyDisk(disk);
    }

    /// Query memory accounting makes the one-byte threshold trigger deterministically.
    void withQueryThread(const std::function<void()> & body)
    {
        MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
        std::thread([&]
        {
            ThreadStatus thread_status;
            thread_status.memory_tracker.setParent(&query);
            thread_status.untracked_memory_limit = 0;
            body();
        }).join();
    }

    DiskPtr disk;
    TemporaryDataOnDiskScopePtr tmp_data;
};

}

TEST_F(ExternalDistinctTransformTest, SpillCompletionIsIndependentOfResultBackpressure)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data);
        auto & transform = connected.transform;
        auto & downstream = connected.downstream;
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        connected.upstream.push(makeChunk({1, 2, 3, 4, 5, 6}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        downstream.setNotNeeded();
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        auto suppression = connected.attachRun();
        ASSERT_TRUE(suppression.sink);
        ASSERT_TRUE(suppression.source);
        auto & suppression_sink = *suppression.sink;
        auto & suppression_source = *suppression.source;
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Ready);
        suppression_sink.work();
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::NeedData);

        /// The three two-row write chunks drain while the six-row hashing result remains blocked.
        for (size_t i = 0; i < 3; ++i)
        {
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();
            ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Ready);
            suppression_sink.work();
            ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::NeedData);
            EXPECT_FALSE(downstream.hasData());
        }

        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Ready);
        EXPECT_EQ(suppression_source.prepare(), IProcessor::Status::NeedData);
        EXPECT_EQ(transform.prepare(), IProcessor::Status::NeedData);

        /// Finalizing the file releases the suppression dependency, even with output still blocked.
        suppression_sink.work();
        ASSERT_EQ(suppression_sink.prepare(), IProcessor::Status::Finished);
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        EXPECT_TRUE(suppression_source.getCompletionPort().isFinished());
        transform.work();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::PortFull);

        downstream.setNeeded();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        ASSERT_TRUE(downstream.hasData());
        EXPECT_EQ(downstream.pull().getNumRows(), 6);
        downstream.setNeeded();

        connected.upstream.push(makeChunk({5, 7, 8, 9}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        auto input = connected.attachRun();
        ASSERT_TRUE(input.sink);
        ASSERT_TRUE(input.source);
        auto & input_sink = *input.sink;
        auto & input_source = *input.source;
        ASSERT_EQ(input_sink.prepare(), IProcessor::Status::Ready);
        input_sink.work();
        ASSERT_EQ(input_sink.prepare(), IProcessor::Status::NeedData);
        for (size_t i = 0; i < 2; ++i)
        {
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();
            ASSERT_EQ(input_sink.prepare(), IProcessor::Status::Ready);
            input_sink.work();
            ASSERT_EQ(input_sink.prepare(), IProcessor::Status::NeedData);
        }

        /// Ordinary input resumes before its file finishes; only the reader waits for that sink.
        EXPECT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        EXPECT_TRUE(connected.upstream.isNeeded());
        EXPECT_FALSE(input_source.getCompletionPort().isFinished());
        EXPECT_EQ(input_source.prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(input_sink.prepare(), IProcessor::Status::Ready);
        input_sink.work();
        EXPECT_EQ(input_sink.prepare(), IProcessor::Status::Finished);
        EXPECT_TRUE(input_source.getCompletionPort().isFinished());
    });
}

TEST_F(ExternalDistinctTransformTest, LimitRetainsPendingResultUntilOutputIsReady)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data, /*limit_hint=*/ 1);
        auto & transform = connected.transform;
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        connected.upstream.push(makeChunk({1, 2}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        connected.downstream.setNotNeeded();
        transform.work();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::PortFull);

        connected.downstream.setNeeded();
        EXPECT_EQ(transform.prepare(), IProcessor::Status::Finished);
        EXPECT_TRUE(connected.upstream.isFinished());
        ASSERT_TRUE(connected.downstream.hasData());
        EXPECT_EQ(connected.downstream.pull().getNumRows(), 2);
        EXPECT_TRUE(connected.downstream.isFinished());
    });
}

TEST_F(ExternalDistinctTransformTest, DownstreamClosureTerminatesRunDependencies)
{
    withQueryThread([&]
    {
        for (bool connect_run : {false, true})
        {
            ConnectedDistinct connected(tmp_data);
            auto & transform = connected.transform;
            ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
            connected.upstream.push(makeChunk({1, 2, 3}));
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            transform.work();
            ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
            if (connect_run)
                connected.attachRun();

            connected.downstream.close();
            EXPECT_EQ(transform.prepare(), IProcessor::Status::Finished);
            for (const auto & output : transform.getOutputs())
                EXPECT_TRUE(output.isFinished());
            for (const auto & input : transform.getInputs())
                EXPECT_TRUE(input.isFinished());
            if (connect_run)
            {
                auto merger = std::ranges::find_if(connected.processors, [](const auto & processor)
                {
                    return typeid_cast<DistinctSortedTransform *>(processor.get());
                });
                ASSERT_NE(merger, connected.processors.end());
                EXPECT_EQ((*merger)->prepare(), IProcessor::Status::Finished);
            }
        }
    });
}

#ifndef DEBUG_OR_SANITIZER_BUILD

/// `LOGICAL_ERROR` aborts during construction in Debug and sanitizer builds. Release enforces the
/// completion protocol with an exception that the caller can observe.
TEST_F(ExternalDistinctTransformTest, CompletionPortRejectsData)
{
    withQueryThread([&]
    {
        ConnectedDistinct connected(tmp_data);
        auto & transform = connected.transform;
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        connected.upstream.push(makeChunk({1}));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        auto run = connected.attachRun();
        ASSERT_TRUE(run.sink);
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::Ready);
        run.sink->work();
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
        transform.work();
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::Ready);
        run.sink->work();
        ASSERT_EQ(run.sink->prepare(), IProcessor::Status::NeedData);
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);

        /// Completion is signaled by closing the port, never by sending a chunk with an empty header.
        run.sink->getCompletionPort().push(Chunk{});
        try
        {
            transform.prepare();
            FAIL() << "Expected an exception for data on the completion port";
        }
        catch (const Exception & exception)
        {
            EXPECT_EQ(exception.code(), ErrorCodes::LOGICAL_ERROR);
        }
    });
}

#endif

TEST_F(ExternalDistinctTransformTest, SpillsBeforeLowCardinalityBitmapAllocation)
{
    MemoryTracker user{&total_memory_tracker, VariableContext::User, false};
    MemoryTracker query{&user, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        const auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(type, "k")});
        constexpr size_t dictionary_size = 200000;
        auto column = type->createColumn();
        for (size_t i = 0; i < dictionary_size; ++i)
            column->insert(Field(std::to_string(i)));
        auto input = column->cut(0, 3);
        column.reset();
        ASSERT_GE(assert_cast<const ColumnLowCardinality &>(*input).getDictionary().size(), dictionary_size);
        ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{},
            /*max_bytes_before_external_distinct_=*/ 1ULL << 30, tmp_data, /*min_free_disk_space_=*/ 0,
            /*max_block_size_rows_=*/ 3, /*preserve_input_order_=*/ false);
        OutputPort upstream{header};
        InputPort downstream{header};
        connect(upstream, transform.getInputs().front());
        connect(transform.getOutputs().front(), downstream);
        downstream.setNeeded();
        ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
        upstream.push(Chunk({std::move(input)}, 3));
        ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);

        {
            /// Spilling must start before allocating a bitmap for the dictionary retained by these
            /// three rows.
            user.setHardLimit(user.get() + 128 * 1024);
            SCOPE_EXIT({ user.setHardLimit(0); });
            ASSERT_NO_THROW(transform.work());
            EXPECT_FALSE(downstream.hasData());
            EXPECT_EQ(transform.prepare(), IProcessor::Status::Ready);
        }

        /// The unconsumed input is collected into an ordinary run without constructing the LC bitmap.
        ASSERT_NO_THROW(transform.work());
        EXPECT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
    }).join();
}

TEST_F(ExternalDistinctTransformTest, SpillsBeforeFilteringWithSpareTableCapacity)
{
    MemoryTracker user{&total_memory_tracker, VariableContext::User, false};
    MemoryTracker query{&user, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto header = std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
            ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "payload")});
        constexpr size_t num_rows = 8;
        auto value = ColumnString::create();
        value->insert(Field(String(1 << 20, 'x')));
        ColumnPtr constant = ColumnConst::create(std::move(value), num_rows);
        const Columns payloads{
            constant->convertToFullColumnIfConst(),
            constant,
            ColumnReplicated::create(assert_cast<const ColumnConst &>(*constant).getDataColumnPtr(),
                ColumnUInt8::create(num_rows, UInt8(0)))};
        for (const auto & payload : payloads)
        {
            SCOPED_TRACE(payload->getName());
            ExternalDistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"},
                /*max_bytes_before_external_distinct_=*/ 1ULL << 30, tmp_data, /*min_free_disk_space_=*/ 0,
                /*max_block_size_rows_=*/ 2, /*preserve_input_order_=*/ false);
            OutputPort upstream{header};
            InputPort downstream{header};
            connect(upstream, transform.getInputs().front());
            connect(transform.getOutputs().front(), downstream);
            downstream.setNeeded();
            auto input = makeChunk({1, 2, 3, 4, 5, 6, 7, 7});
            input.addColumn(payload);
            size_t materialization_bytes = 0;
            size_t filtering_bytes = 0;
            {
                auto prepared = input.clone();
                materializeChunk(prepared);
                filtering_bytes = prepared.allocatedBytes();
                if (prepared.getColumns()[1] != payload)
                    materialization_bytes = prepared.getColumns()[1]->allocatedBytes();
            }
            ASSERT_EQ(transform.prepare(), IProcessor::Status::NeedData);
            upstream.push(std::move(input));
            ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
            {
                /// The table needs no growth, but filtering would copy most of the wide payload.
                user.setHardLimit(user.get() + materialization_bytes + filtering_bytes / 4 + 65536);
                SCOPE_EXIT({ user.setHardLimit(0); });
                ASSERT_NO_THROW(transform.work());
                ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
                EXPECT_FALSE(downstream.hasData());
            }

            /// The unconsumed input is collected into an ordinary run after switching to spilling.
            ASSERT_NO_THROW(transform.work());
            EXPECT_EQ(transform.prepare(), IProcessor::Status::UpdatePipeline);
        }
    }).join();
}
