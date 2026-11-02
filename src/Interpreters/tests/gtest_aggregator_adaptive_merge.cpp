#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <map>
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

AggregateDescription makeAggregate(const String & name, const Names & argument_names, const DataTypes & argument_types)
{
    AggregateDescription description;
    AggregateFunctionProperties properties;
    description.function = AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, argument_types, {}, properties);
    description.argument_names = argument_names;
    description.column_name = name;
    return description;
}

/// The parameters of an adaptive aggregation whose tables freeze at their first key.
Aggregator::Params makeAdaptiveParams(const Names & keys, const AggregateDescriptions & aggregates)
{
    return Aggregator::Params(
        keys, aggregates,
        /*overflow_row_=*/false, /*max_rows_to_group_by_=*/0, OverflowMode::THROW,
        /*group_by_two_level_threshold_=*/0, /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0, /*min_bytes_before_external_group_by_=*/0,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/nullptr, /*max_threads_=*/3, /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false, /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536, /*enable_prefetch_=*/false, /*only_merge_=*/false,
        /*optimize_group_by_constant_keys_=*/true,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5, StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/false,
        /*serialize_string_with_zero_byte_=*/false, /*enable_parallel_single_level_merge_=*/false,
        /*enable_packed_string_keys_=*/true, /*enable_adaptive_aggregator_=*/true,
        /*adaptive_aggregator_freeze_threshold_=*/1, /*adaptive_aggregator_freeze_threshold_bytes_=*/0,
        /*adaptive_aggregator_disable_thaw_=*/false, /*use_aggregation_memory_tracker_=*/true);
}

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
            aggregates.push_back(makeAggregate("count", {}, {}));
        for (const auto & function : {first, second})
        {
            AggregateDescription description;
            description.function = function;
            description.column_name = aggregates.empty() ? "first" : "next" + std::to_string(aggregates.size());
            aggregates.push_back(std::move(description));
        }

        {
            auto params = makeAdaptiveParams({"k"}, aggregates);
            if (count_first)
                params.bucket_top_k = 1;

            const auto type = std::make_shared<DataTypeUInt64>();
            const Block header({{type->createColumn(), type, "k"}});
            Aggregator aggregator(header, params);
            auto session = aggregator.createAdaptiveSession(3);
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
                sources.push_back(std::move(source));
            }

            auto & table = sources.front()->key64_two_level->data;
            const auto bucket = static_cast<Int32>(
                AggregatedDataWithUInt64KeyTwoLevel::getBucketFromHash(table.hash(UInt64{1})));
            aggregator.prepareAdaptiveTopKPruning(*session, sources.size(), /*external_merge=*/false, /*collects_statistics=*/false);
            auto prepared = aggregator.prepareVariantsToMerge(std::move(sources), session.get());
            AdaptiveMergeTask task;
            std::atomic<bool> cancelled{false};
            const auto merge = [&]
            {
                return aggregator.mergeAndConvertAdaptiveBucket(
                    prepared, *session, task, /*final=*/true, bucket, cancelled, /*updater=*/nullptr, /*full_group_count=*/nullptr);
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

/// A block whose keys are all constant reads its one key for every row. Each producer learns key `a` from its first
/// block and freezes. Its second block, of key `b`, misses on every row and stages a record per row, each with the key.
/// The `Array` key is serialized row by row, so the batch of misses keeps the bytes of the key, once for all the
/// records. The merge returns both groups with all their rows.
TEST(AggregatorAdaptiveStaging, ConstantSerializedKey)
{
    MainThreadStatus::getInstance();
    tryRegisterAggregateFunctions();

    const auto number = std::make_shared<DataTypeUInt64>();
    const auto string = std::make_shared<DataTypeString>();
    const auto array = std::make_shared<DataTypeArray>(number);
    const AggregateDescriptions aggregates{makeAggregate("count", {}, {}), makeAggregate("sum", {"n"}, {number})};
    const Block header({{string->createColumn(), string, "s"}, {array->createColumn(), array, "k"}, {number->createColumn(), number, "n"}});
    Aggregator aggregator(header, makeAdaptiveParams({"s", "k"}, aggregates));

    constexpr size_t rows = 1000;
    const auto block = [&](char key, UInt64 element)
    {
        auto values = ColumnUInt64::create();
        for (size_t i = 0; i < rows; ++i)
            values->insertValue(i);
        return Columns{
            string->createColumnConst(rows, String(100, key)), array->createColumnConst(rows, Array{Field(element)}), std::move(values)};
    };

    auto session = aggregator.createAdaptiveSession(2);
    ManyAggregatedDataVariants sources;
    for (size_t i = 0; i < 2; ++i)
    {
        auto source = std::make_shared<AggregatedDataVariants>();
        AdaptiveAggregationProducer producer(session);
        ColumnRawPtrs key_columns(2);
        Aggregator::AggregateColumns aggregate_columns(aggregates.size());
        bool no_more_keys = false;
        ASSERT_TRUE(aggregator.executeOnBlock(block('a', 1), 0, rows, *source, key_columns, aggregate_columns, no_more_keys, &producer));
        ASSERT_TRUE(producer.isFrozen());
        ASSERT_TRUE(aggregator.executeOnBlock(block('b', 2), 0, rows, *source, key_columns, aggregate_columns, no_more_keys, &producer));
        EXPECT_EQ(producer.total_staged_records, rows);
        /// A copy of the key per record would have grown the buffer past the bytes of all the keys.
        EXPECT_LT(producer.misses.getKeys().allocated_bytes(), rows * 100);
        aggregator.finishAdaptiveProducer(*source, producer);
        sources.push_back(std::move(source));
    }

    aggregator.prepareAdaptiveTopKPruning(*session, sources.size(), /*external_merge=*/false, /*collects_statistics=*/false);
    auto prepared = aggregator.prepareVariantsToMerge(std::move(sources), session.get());
    AdaptiveMergeTask task;
    std::atomic<bool> cancelled{false};
    std::map<std::pair<String, UInt64>, std::pair<UInt64, UInt64>> groups;
    for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
    {
        const auto chunks = aggregator.mergeAndConvertAdaptiveBucket(
            prepared, *session, task, /*final=*/true, static_cast<Int32>(bucket), cancelled,
            /*updater=*/nullptr, /*full_group_count=*/nullptr);
        for (const auto & converted : chunks)
        {
            const auto & columns = converted.chunk.getColumns();
            for (size_t row = 0; row < converted.chunk.getNumRows(); ++row)
                groups[{String(columns[0]->getDataAt(row)), (*columns[1])[row].safeGet<Array>().at(0).safeGet<UInt64>()}]
                    = {columns[2]->getUInt(row), columns[3]->getUInt(row)};
        }
    }

    /// Both producers add every row of both keys.
    const std::pair<UInt64, UInt64> all_rows{2 * rows, rows * (rows - 1)};
    const decltype(groups) expected{{{String(100, 'a'), 1}, all_rows}, {{String(100, 'b'), 2}, all_rows}};
    EXPECT_EQ(groups, expected);
}
