#include <gtest/gtest.h>

#include <Interpreters/ProcessorsProfileLog.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

/// Minimal processor with a single output port. Used as the surviving upstream.
class OneOutputProcessor final : public IProcessor
{
public:
    explicit OneOutputProcessor(SharedHeader header) : IProcessor({}, {Block(*header)}) { }
    String getName() const override { return "OneOutputProcessor"; }
    Status prepare() override { return Status::Finished; }
};

/// Minimal processor with a single input port. Used as the downstream peer.
class OneInputProcessor final : public IProcessor
{
public:
    explicit OneInputProcessor(SharedHeader header) : IProcessor({Block(*header)}, {}) { }
    String getName() const override { return "OneInputProcessor"; }
    Status prepare() override { return Status::Finished; }
};

}

/// Regression test for the release-reachable heap-use-after-free in getProcessorsProfileLogInfo
/// (follow-up to #110955 / issue #110834, which fixed the same shape in the debug-only printPipeline).
///
/// A surviving processor keeps an output port that is still isConnected() after its downstream peer
/// processor has been destroyed: the shared Port::State is ref-counted, so the survivor's side stays
/// connected, and the raw peer pointer is not cleared on destruction. Building parent_ids by following
/// that output port to its peer processor dereferenced a freed processor (heap-use-after-free under
/// ASAN), on a release-reachable path (executeQuery.cpp logQueryFinish, scalar-subquery logging).
TEST(ProcessorsProfileLog, DoesNotDereferenceDanglingOutputPeer)
{
    auto header = makeHeader();

    auto upstream = std::make_shared<OneOutputProcessor>(header);
    auto downstream = std::make_shared<OneInputProcessor>(header);

    connect(upstream->getOutputs().front(), downstream->getInputs().front());
    ASSERT_TRUE(upstream->getOutputs().front().isConnected());

    /// Destroy the downstream peer while the survivor's output port stays connected.
    downstream.reset();
    ASSERT_TRUE(upstream->getOutputs().front().isConnected());

    /// Only the survivor is passed in -- exactly what remains when a downstream processor was
    /// removed/destroyed during pipeline teardown.
    Processors processors{upstream};

    auto infos = getProcessorsProfileLogInfo(processors);

    ASSERT_EQ(infos.size(), 1u);
    /// The dangling peer is not in the set, so it contributes no parent id (and is never dereferenced).
    EXPECT_TRUE(infos[0].parent_ids.empty());
}

/// A well-formed pipeline (both processors present) still records the parent link, so the fix does not
/// regress the parent_ids stored in system.processors_profile_log for normal queries.
TEST(ProcessorsProfileLog, RecordsParentIdForProcessorsInSet)
{
    auto header = makeHeader();

    auto upstream = std::make_shared<OneOutputProcessor>(header);
    auto downstream = std::make_shared<OneInputProcessor>(header);
    connect(upstream->getOutputs().front(), downstream->getInputs().front());

    Processors processors{upstream, downstream};

    auto infos = getProcessorsProfileLogInfo(processors);

    ASSERT_EQ(infos.size(), 2u);

    const auto upstream_id = reinterpret_cast<std::uintptr_t>(upstream.get());
    const auto downstream_id = reinterpret_cast<std::uintptr_t>(downstream.get());

    /// upstream's only output feeds downstream's input, so upstream lists downstream as its parent.
    ASSERT_EQ(infos[0].id, upstream_id);
    ASSERT_EQ(infos[0].parent_ids.size(), 1u);
    EXPECT_EQ(infos[0].parent_ids[0], downstream_id);

    /// downstream has no outputs, so it has no parents.
    ASSERT_EQ(infos[1].id, downstream_id);
    EXPECT_TRUE(infos[1].parent_ids.empty());
}
