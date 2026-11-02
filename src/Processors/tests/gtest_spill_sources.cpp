#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <tuple>
#include <vector>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/tests/gtest_disk.h>
#include <Processors/Transforms/BufferingFileTransforms.h>
#include <Processors/Transforms/SortingTransform.h>

using namespace DB;

namespace
{

enum class SourceKind
{
    File,
    SortedChunks,
    UniqueChunks,
};

class SpillSourceTest : public testing::TestWithParam<std::tuple<SourceKind, IProcessor::CancelReason, bool>>
{
protected:
    SharedHeader header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key")});
    OutputPort completion{Block{}};
    DiskPtr disk;
    TemporaryDataOnDiskScopePtr tmp_data;
    std::optional<TemporaryBlockStreamHolder> stream;

    void TearDown() override
    {
        stream.reset();
        tmp_data.reset();
        destroyDisk(disk);
    }

    std::unique_ptr<ISource> makeSource(SourceKind kind)
    {
        Chunks chunks;
        for (UInt64 first : {1, 3, 5})
        {
            auto column = ColumnUInt64::create();
            column->insertValue(first);
            column->insertValue(first + 1);
            chunks.emplace_back(Columns{std::move(column)}, 2);
        }

        if (kind == SourceKind::File)
        {
            disk = createDisk("spill_source");
            tmp_data = std::make_shared<TemporaryDataOnDiskScope>(
                TemporaryDataOnDiskSettings{}, std::make_shared<SingleDiskVolume>("temporary", disk));
            stream.emplace(header, tmp_data, 0);
            for (auto & chunk : chunks)
                (*stream)->write(header->cloneWithColumns(chunk.detachColumns()));
            stream->finishWriting();

            auto source = std::make_unique<BufferingFromFileSource>(header, *stream, getLogger("SpillSourceTest"));
            connect(completion, source->getCompletionPort());
            completion.finish();
            return source;
        }

        SortDescription description;
        description.emplace_back("key", 1, 1);
        auto mode = kind == SourceKind::UniqueChunks ? MergeSorter::Mode::MergeUniqueChunks : MergeSorter::Mode::PreserveRows;
        return std::make_unique<MergeSorterSource>(header, std::move(chunks), description, 2, 0, mode);
    }
};

}

TEST_P(SpillSourceTest, CancellationPreservesTheRequestedResult)
{
    const auto [kind, reason, cancel_with_pending_chunk] = GetParam();
    auto source = makeSource(kind);
    InputPort downstream{header};
    connect(source->getPort(), downstream);
    downstream.setNeeded();

    if (cancel_with_pending_chunk)
    {
        ASSERT_EQ(source->prepare(), IProcessor::Status::Ready);
        source->work();
        downstream.setNotNeeded();
    }

    /// A later cancellation must still take effect after requesting a partial result.
    source->cancel(IProcessor::CancelReason::PartialResult);
    source->cancel(reason);
    const bool partial_result = reason == IProcessor::CancelReason::PartialResult;
    EXPECT_EQ(source->isCancelled(), !partial_result);

    if (cancel_with_pending_chunk)
    {
        EXPECT_EQ(source->prepare(), IProcessor::Status::PortFull);
        EXPECT_FALSE(downstream.hasData());
        downstream.setNeeded();
    }

    std::vector<UInt64> values;
    for (size_t step = 0; step < 16 && !downstream.isFinished(); ++step)
    {
        auto status = source->prepare();
        if (status == IProcessor::Status::Ready)
            source->work();
        else if (status == IProcessor::Status::PortFull || status == IProcessor::Status::Finished)
        {
            if (downstream.hasData())
            {
                auto chunk = downstream.pull();
                for (size_t row = 0; row < chunk.getNumRows(); ++row)
                    values.push_back(chunk.getColumns().front()->getUInt(row));
            }
            else
                ASSERT_TRUE(downstream.isFinished());
        }
        else
            FAIL() << "Unexpected source status: " << static_cast<int>(status);
    }

    ASSERT_TRUE(downstream.isFinished());
    /// Partial results drain every buffered block. Other cancellation reasons stop after the next block.
    const std::vector<UInt64> expected = partial_result ? std::vector<UInt64>{1, 2, 3, 4, 5, 6} : std::vector<UInt64>{1, 2};
    EXPECT_EQ(values, expected);
}

INSTANTIATE_TEST_SUITE_P(
    Replay,
    SpillSourceTest,
    testing::Combine(
        testing::Values(SourceKind::File, SourceKind::SortedChunks, SourceKind::UniqueChunks),
        testing::Values(
            IProcessor::CancelReason::PartialResult,
            IProcessor::CancelReason::Unknown,
            IProcessor::CancelReason::CancelledByUser,
            IProcessor::CancelReason::CancelledByTimeout,
            IProcessor::CancelReason::Exception),
        testing::Bool()));
