#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <base/unit.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <memory>
#include <optional>
#include <string>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

const String filter_key = "runtime_filter_key";

/// `Array(String)` is not unambiguously representable in a contiguous memory region, so the
/// transform builds the exact-set variant of the runtime filter (the one in the reported abort)
/// instead of the adaptive bloom-filter variant.
DataTypePtr makeKeyType()
{
    tryRegisterFunctions();
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
}

SharedHeader makeHeader(const DataTypePtr & key_type)
{
    Block header{ColumnWithTypeAndName(key_type, "key")};
    return std::make_shared<const Block>(std::move(header));
}

/// `count` distinct single-element arrays starting at `first_key`.
Chunk makeKeyChunk(const DataTypePtr & key_type, UInt64 first_key, UInt64 count)
{
    auto column = key_type->createColumn();
    for (UInt64 i = 0; i < count; ++i)
        column->insert(Array{std::to_string(first_key + i)});
    return Chunk(Columns{std::move(column)}, count);
}

ColumnWithTypeAndName makeKeyColumnWithType(const DataTypePtr & key_type, const std::vector<UInt64> & keys)
{
    auto column = key_type->createColumn();
    for (const auto key : keys)
        column->insert(Array{std::to_string(key)});
    return ColumnWithTypeAndName(std::move(column), key_type, "key");
}

/// A query context holding the lookup the transform registers into, plus the thread group that
/// `__applyFilter` resolves that context through.
struct QueryScope
{
    RuntimeFilterLookupPtr lookup = createRuntimeFilterLookup();
    ContextMutablePtr query_context;
    std::optional<ThreadStatus> thread_status;
    ThreadGroupPtr thread_group;
    std::optional<ThreadGroupSwitcher> thread_group_switcher;

    QueryScope()
    {
        query_context = Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setRuntimeFilterLookup(lookup);

        /// Some unit-test configurations initialize a thread status before this test, others leave
        /// `current_thread` unset. Reuse the existing one when present instead of replacing it.
        if (!CurrentThread::isInitialized())
            thread_status.emplace();
        thread_group = std::make_shared<ThreadGroup>(query_context, 0);
        thread_group_switcher.emplace(thread_group, ThreadName::UNKNOWN, /*allow_existing_group=*/true);
    }
};

std::shared_ptr<BuildRuntimeFilterTransform> makeTransform(const QueryScope & scope, const DataTypePtr & key_type)
{
    return std::make_shared<BuildRuntimeFilterTransform>(
        makeHeader(key_type),
        "key",
        key_type,
        /*filter_name_=*/"runtime_filter",
        filter_key,
        /*filters_to_merge_=*/0,
        /*exact_values_limit_=*/100000,
        /*bloom_filter_bytes_=*/1_MiB,
        /*bloom_filter_hash_functions_=*/3,
        /*pass_ratio_threshold_for_disabling_=*/1.0,
        /*blocks_to_skip_before_reenabling_=*/30,
        /*max_ratio_of_set_bits_in_bloom_filter_=*/1.0,
        /*allow_to_use_not_exact_filter_=*/true,
        /*track_key_range_=*/false,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false,
        scope.query_context);
}

/// `transform(Chunk &)` and `transform(std::exception_ptr &)` are public on `ISimpleTransform`,
/// so the test drives them through the base reference exactly as the executor does.
void runTransform(ISimpleTransform & transform, Chunk & chunk)
{
    transform.transform(chunk);
}

void runTransform(ISimpleTransform & transform, std::exception_ptr & exception)
{
    transform.transform(exception);
}

/// Peers for the transform's own ports. `Port` methods require a connected peer, and closing the
/// downstream input is what makes `ISimpleTransform::prepare` report `Finished`, which is where
/// `BuildRuntimeFilterTransform` registers its filter.
struct ConnectedPorts
{
    OutputPort upstream;
    InputPort downstream;

    ConnectedPorts(ISimpleTransform & transform, const SharedHeader & header)
        : upstream(header), downstream(header)
    {
        connect(upstream, transform.getInputPort());
        connect(transform.getOutputPort(), downstream);
    }
};

/// Runs the registration path the pipeline reaches once the consumer closes its input.
void finishByClosingDownstream(ISimpleTransform & transform, ConnectedPorts & ports)
{
    ports.downstream.close();
    ASSERT_EQ(transform.prepare(), IProcessor::Status::Finished);
}

/// Deterministic allocation failure: with every allocation reported to the tracker, a hard limit
/// just above current usage makes the next one inside the transform throw.
struct FailingAllocations
{
    MemoryTracker & thread_tracker = CurrentThread::get().memory_tracker;
    Int64 prev_hard_limit = thread_tracker.getHardLimit();
    Int64 prev_untracked_limit = CurrentThread::get().untracked_memory_limit;

    FailingAllocations()
    {
        CurrentThread::flushUntrackedMemory();
        CurrentThread::get().untracked_memory_limit = 1;
        thread_tracker.setHardLimit(thread_tracker.get() + 64 * 1024);
    }

    ~FailingAllocations() { release(); }

    /// Lift the limit before anything else runs, so cleanup and later chunks are unaffected.
    void release()
    {
        thread_tracker.setHardLimit(prev_hard_limit);
        CurrentThread::get().untracked_memory_limit = prev_untracked_limit;
        CurrentThread::flushUntrackedMemory();
    }
};

}

/// Regression test for the server abort "Logical error: 'offsets_column has data inconsistent with
/// nested_column'" reached from `ExactSetRuntimeFilter::insert` (STID 2632-398f).
///
/// `ISimpleTransform::work` converts an exception thrown by `transform` into port data and keeps the
/// transform alive, so the build stream still reaches `prepare() == Finished`. Registering there
/// handed the half-built filter to `RuntimeFilterLookup::add`, whose merge reads the set's value
/// columns - one of which the interrupted insert had left mid-append - and aborted the server.
TEST(BuildRuntimeFilterTransform, FailedBuildRegistersNothingAndIsReentrySafe)
{
    const auto key_type = makeKeyType();
    QueryScope scope;
    auto transform = makeTransform(scope, key_type);
    ConnectedPorts ports(*transform, makeHeader(key_type));

    Chunk first_chunk = makeKeyChunk(key_type, /*first_key=*/0, /*count=*/50000);

    bool threw = false;
    {
        FailingAllocations failing_allocations;
        try
        {
            runTransform(*transform, first_chunk);
        }
        catch (const Exception & e)
        {
            threw = (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED);
        }
        failing_allocations.release();
    }
    ASSERT_TRUE(threw) << "expected the first chunk to hit the memory limit inside the filter insert";

    /// Re-entry: the executor can hand the transform another chunk after the throw. The abandoned
    /// build must pass it through instead of touching the released filter.
    Chunk second_chunk = makeKeyChunk(key_type, /*first_key=*/1000000, /*count=*/4);
    ASSERT_NO_THROW(runTransform(*transform, second_chunk));
    EXPECT_EQ(second_chunk.getNumRows(), 4u);

    finishByClosingDownstream(*transform, ports);
    EXPECT_EQ(scope.lookup->find(filter_key), nullptr) << "a failed build must not be registered";
}

/// An exception arriving from upstream reaches the build side as port data, so this stream never saw
/// the rows it still owes and its key set is incomplete.
TEST(BuildRuntimeFilterTransform, UpstreamExceptionRegistersNothing)
{
    const auto key_type = makeKeyType();
    QueryScope scope;
    auto transform = makeTransform(scope, key_type);
    ConnectedPorts ports(*transform, makeHeader(key_type));

    Chunk chunk = makeKeyChunk(key_type, /*first_key=*/0, /*count=*/4);
    ASSERT_NO_THROW(runTransform(*transform, chunk));

    auto exception = std::make_exception_ptr(Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "upstream failed"));
    runTransform(*transform, exception);

    finishByClosingDownstream(*transform, ports);
    EXPECT_EQ(scope.lookup->find(filter_key), nullptr) << "an incomplete build must not be registered";
}

/// Anti-vacuity: a build that completes must still register a filter that actually filters, so the
/// assertions above cannot be satisfied by never registering anything.
TEST(BuildRuntimeFilterTransform, SuccessfulBuildRegistersFilterThatFilters)
{
    const auto key_type = makeKeyType();
    QueryScope scope;
    auto transform = makeTransform(scope, key_type);
    ConnectedPorts ports(*transform, makeHeader(key_type));

    Chunk chunk = makeKeyChunk(key_type, /*first_key=*/1, /*count=*/3);
    ASSERT_NO_THROW(runTransform(*transform, chunk));

    finishByClosingDownstream(*transform, ports);
    auto filter = scope.lookup->find(filter_key);
    ASSERT_NE(filter, nullptr);
    EXPECT_TRUE(filter->isReady());

    /// Probe through `__applyFilter`, which resolves the filter by the same rendezvous key the
    /// transform registered under. Keys 2 and 3 were built, 100 and 101 were not.
    const auto id_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments{
        {id_type->createColumnConst(4, filter_key), id_type, "filter_id"},
        makeKeyColumnWithType(key_type, {2, 3, 100, 101})};
    auto apply_filter = FunctionFactory::instance().get("__applyFilter", scope.query_context)->build(arguments);
    auto mask = apply_filter->execute(arguments, apply_filter->getResultType(), 4, false);

    ASSERT_EQ(mask->size(), 4u);
    EXPECT_TRUE(mask->getBool(0));
    EXPECT_TRUE(mask->getBool(1));
    EXPECT_FALSE(mask->getBool(2));
    EXPECT_FALSE(mask->getBool(3));
}
