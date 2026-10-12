#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <stdexcept>
#include <tuple>

using namespace DB;

namespace
{

enum class MergeMode
{
    Batch,
    Pairwise,
    Group,
};

/// Tracks state lifetimes through the ordinary batch merge and the adaptive pairwise and group merges.
/// An exception on the second merge also exercises cleanup after an earlier source merged successfully.
class TrackedMergeFunction : public IAggregateFunctionDataHelper<UInt64, TrackedMergeFunction>
{
public:
    TrackedMergeFunction(MergeMode mode_, bool throws_)
        : IAggregateFunctionDataHelper({}, {}, std::make_shared<DataTypeUInt64>()), mode(mode_), throws(throws_)
    {
    }

    String getName() const override { return "trackedMerge"; }
    size_t sizeOfData() const override { return sizeof(UInt64); }
    size_t alignOfData() const override { return alignof(UInt64); }
    bool hasTrivialDestructor() const override { return false; }
    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr, const IColumn **, size_t, Arena *) const override { }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & out, std::optional<size_t>) const override
    {
        writeBinary(data(place), out);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & in, std::optional<size_t>, Arena *) const override
    {
        readBinary(data(place), in);
    }

    void insertResultInto(AggregateDataPtr, IColumn & column, Arena *) const override { column.insertDefault(); }

    void create(AggregateDataPtr place) const override
    {
        data(place) = 1;
        ++live_states;
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        EXPECT_EQ(data(place), 1);
        data(place) = 0;
        --live_states;
    }

    void mergeImpl(AggregateDataPtr, ConstAggregateDataPtr, Arena *) const override
    {
        if (++merge_calls == 2 && throws)
            throw std::runtime_error("State merge failed");
    }

    bool isAbleToParallelizeMerge() const override { return mode != MergeMode::Batch; }
    bool isParallelizeMergePrepareNeeded() const override { return mode != MergeMode::Batch; }

    size_t getEstimatedMergeWork(ConstAggregateDataPtr) const override
    {
        return mode == MergeMode::Group ? adaptive_parallel_merge_max_work + 1 : 1;
    }

    void parallelizeMergePrepare(AggregateDataPtrs &, ThreadPool &, std::atomic<bool> &) const override { }

    void parallelizeMergeMulti(
        AggregateDataPtrs & places, ThreadPool &, std::atomic<bool> &, Arena * arena) const override
    {
        ++group_merges;
        for (size_t i = 1; i < places.size(); ++i)
            merge(places[0], places[i], arena);
    }

    mutable size_t live_states = 0;
    mutable size_t merge_calls = 0;
    mutable size_t group_merges = 0;

private:
    MergeMode mode;
    bool throws;
};

class AggregatorAdaptiveMerge : public ::testing::TestWithParam<std::tuple<MergeMode, bool>>
{
protected:
    void SetUp() override
    {
        MainThreadStatus::getInstance();
        tryRegisterAggregateFunctions();
    }
};

}

TEST_P(AggregatorAdaptiveMerge, SourceStateLifetimes)
{
    const auto [mode, count_first] = GetParam();
    for (const bool throws : {false, true})
    {
        SCOPED_TRACE(throws);
        auto first = std::make_shared<TrackedMergeFunction>(mode, false);
        auto second = std::make_shared<TrackedMergeFunction>(mode, throws);
        AggregateDescriptions aggregates;
        if (count_first)
        {
            AggregateFunctionProperties properties;
            AggregateDescription count;
            count.function = AggregateFunctionFactory::instance().get("count", NullsAction::EMPTY, {}, {}, properties);
            count.column_name = "count";
            aggregates.push_back(std::move(count));
        }
        for (const auto & function : {first, second})
        {
            AggregateDescription description;
            description.function = function;
            description.column_name = aggregates.empty() ? "first" : "next" + std::to_string(aggregates.size());
            aggregates.push_back(std::move(description));
        }

        {
            Aggregator::Params params(
                {"k"}, aggregates,
                /*overflow_row_=*/false, /*max_rows_to_group_by_=*/0, OverflowMode::THROW,
                /*group_by_two_level_threshold_=*/0, /*group_by_two_level_threshold_bytes_=*/0,
                /*max_bytes_before_external_group_by_=*/0, /*empty_result_for_aggregation_by_empty_set_=*/false,
                /*tmp_data_scope_=*/nullptr, /*max_threads_=*/3, /*min_free_disk_space_=*/0,
                /*compile_aggregate_expressions_=*/false, /*min_count_to_compile_aggregate_expression_=*/0,
                /*max_block_size_=*/65536, /*enable_prefetch_=*/false, /*only_merge_=*/false,
                /*optimize_group_by_constant_keys_=*/true,
                /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5, StatsCollectingParams{},
                /*enable_producing_buckets_out_of_order_in_aggregation_=*/false,
                /*serialize_string_with_zero_byte_=*/false, /*enable_parallel_single_level_merge_=*/false,
                /*enable_packed_string_keys_=*/true, /*enable_adaptive_aggregator_=*/true,
                /*adaptive_aggregator_freeze_threshold_=*/1, /*adaptive_aggregator_freeze_threshold_bytes_=*/0,
                /*adaptive_aggregator_disable_thaw_=*/false);
            if (count_first)
                params.bucket_top_k = 1;

            const auto type = std::make_shared<DataTypeUInt64>();
            const Block header({{type->createColumn(), type, "k"}});
            Aggregator aggregator(header, params);
            auto session = std::make_shared<AdaptiveAggregationSession>(AdaptivePartitionLayout::forProducers(3, 0));
            ManyAggregatedDataVariants sources;
            for (size_t i = 0; i < 3; ++i)
            {
                auto source = std::make_shared<AggregatedDataVariants>();
                AdaptiveAggregationProducer producer(session);
                ColumnRawPtrs key_columns(1);
                Aggregator::AggregateColumns aggregate_columns(aggregates.size());
                auto key = ColumnUInt64::create();
                key->insertValue(1);
                bool no_more_keys = false;
                ASSERT_TRUE(aggregator.executeOnBlock(
                    {std::move(key)}, 0, 1, *source, key_columns, aggregate_columns, no_more_keys, &producer));
                ASSERT_TRUE(producer.isFrozen());
                aggregator.finishAdaptiveProducer(*source, producer);
                source->convertToTwoLevel();
                sources.push_back(std::move(source));
            }

            auto & table = sources.front()->key64_two_level->data;
            const auto bucket = static_cast<Int32>(
                AggregatedDataWithUInt64KeyTwoLevel::getBucketFromHash(table.hash(UInt64{1})));
            auto prepared = aggregator.prepareVariantsToMerge(std::move(sources), session.get());
            prepared.front()->adaptive_merge_bucket_arenas.resize(ADAPTIVE_AGGREGATION_NUM_BUCKETS);
            for (auto & arena : prepared.front()->adaptive_merge_bucket_arenas)
                arena = std::make_shared<Arena>();
            AdaptiveMergeScratch scratch;
            std::atomic<bool> cancelled{false};
            const auto merge = [&]
            {
                return aggregator.mergeAndConvertAdaptiveBucket(
                    prepared, *session, scratch, /*final=*/true, bucket, /*previous_bucket=*/-1,
                    cancelled, /*updater=*/nullptr, /*full_group_count=*/nullptr);
            };
            if (throws)
                EXPECT_THROW(merge(), std::runtime_error);
            else
                EXPECT_EQ(merge().front().chunk.getNumRows(), 1);
            EXPECT_EQ(second->merge_calls, 2);
            EXPECT_EQ(second->group_merges, mode == MergeMode::Group ? 1 : 0);
        }

        EXPECT_EQ(first->live_states, 0);
        EXPECT_EQ(second->live_states, 0);
    }
}

INSTANTIATE_TEST_SUITE_P(MergePaths, AggregatorAdaptiveMerge,
    ::testing::Combine(::testing::Values(MergeMode::Batch, MergeMode::Pairwise, MergeMode::Group), ::testing::Bool()));
