#include <gtest/gtest.h>

#include <bit>
#include <limits>
#include <map>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Aggregator.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// The parallel single-level merge (`Aggregator::mergeSingleLevelPartitionAndConvertToChunk`) combines
/// the per-thread hash tables partition by partition, where a key's partition is its two-level bucket modulo the
/// partition count. Each test builds several tables through `executeOnBlock`, merges every partition, and
/// asserts that the partitions partition the keys and that their union equals a serial reference
/// aggregation of the same data. Each test targets one dispatched method of the merge.

ColumnPtr makeColumn(const DataTypePtr & type, const std::vector<Field> & values)
{
    auto column = type->createColumn();
    for (const auto & value : values)
        column->insert(value);
    return column;
}

AggregateDescription makeAggregate(const String & name, const Names & argument_names, const DataTypes & argument_types)
{
    AggregateDescription description;
    AggregateFunctionProperties properties;
    description.function = AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, argument_types, {}, properties);
    description.argument_names = argument_names;
    description.column_name = name;
    return description;
}

Aggregator::Params makeParams(const Names & keys, const AggregateDescriptions & aggregates)
{
    return Aggregator::Params(
        keys,
        aggregates,
        /*overflow_row_=*/false,
        /*max_rows_to_group_by_=*/0,
        OverflowMode::THROW,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/nullptr,
        /*max_threads_=*/2,
        /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false,
        /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536,
        /*enable_prefetch_=*/true,
        /*only_merge_=*/false,
        /*optimize_group_by_constant_keys_=*/true,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/false,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_parallel_single_level_merge_=*/true,
        /*enable_packed_string_keys_=*/true,
        /*enable_adaptive_aggregator_=*/false,
        /*adaptive_aggregator_freeze_threshold_=*/0,
        /*adaptive_aggregator_freeze_threshold_bytes_=*/0);
}

/// Renders one finalized chunk as "key[,key]" -> "value[,value]" rows.
std::map<String, String> renderChunk(const Chunk & chunk, size_t keys_size)
{
    std::map<String, String> result;
    const auto & columns = chunk.getColumns();
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        String key;
        String value;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            String & target = i < keys_size ? key : value;
            if (!target.empty())
                target += ",";
            target += applyVisitor(FieldVisitorToString(), (*columns[i])[row]);
        }
        result[key] = value;
    }
    return result;
}

/// One aggregation scenario: the same blocks aggregated as several per-thread tables and merged by
/// partitions, versus aggregated into one table serially as the reference.
struct Scenario
{
    Block header;
    Aggregator::Params params;
    Aggregator aggregator;

    Scenario(Block header_, Aggregator::Params params_)
        : header(std::move(header_)), params(std::move(params_)), aggregator(header, params)
    {
    }

    AggregatedDataVariantsPtr aggregate(const std::vector<Columns> & blocks) const
    {
        auto variants = std::make_shared<AggregatedDataVariants>();
        ColumnRawPtrs key_columns(params.keys_size);
        Aggregator::AggregateColumns aggregate_columns(params.aggregates_size);
        bool no_more_keys = false;
        for (const auto & block : blocks)
        {
            const size_t rows = block.front()->size();
            aggregator.executeOnBlock(block, 0, rows, *variants, key_columns, aggregate_columns, no_more_keys, /*adaptive=*/nullptr);
        }
        return variants;
    }

    /// Merges `sources` partition by partition and returns the union, asserting the partitions are key-disjoint.
    std::map<String, String> mergePartitions(std::vector<AggregatedDataVariantsPtr> sources, size_t num_partitions) const
    {
        ManyAggregatedDataVariants many(sources.begin(), sources.end());
        std::atomic<bool> cancelled{false};
        auto prepared = aggregator.prepareVariantsToMerge(std::move(many), cancelled, /*adaptive_session=*/nullptr);
        EXPECT_FALSE(prepared.empty());
        EXPECT_FALSE(prepared.at(0)->isTwoLevel());
        EXPECT_TRUE(aggregator.canMergeSingleLevelInPartitions(*prepared.at(0)));

        size_t max_table_size = 0;
        for (const auto & variants : prepared)
            max_table_size = std::max(max_table_size, variants->sizeWithoutOverflowRow());
        std::map<String, String> united;
        for (size_t partition = 0; partition < num_partitions; ++partition)
        {
            auto agg_chunk = aggregator.mergeSingleLevelPartitionAndConvertToChunk(
                prepared, /*final=*/true, partition, num_partitions, max_table_size, cancelled, /*updater=*/nullptr);
            for (const auto & [key, value] : renderChunk(agg_chunk.chunk, params.keys_size))
            {
                EXPECT_TRUE(united.emplace(key, value).second) << "key " << key << " produced by two partitions";
            }
        }
        return united;
    }

    /// Merges partition by partition without finalizing — the chunks carry aggregate state columns whose
    /// states (including the adopted ones) must stay alive through the chunk — then finalizes the
    /// state columns and renders them like the final merge would.
    std::map<String, String> mergePartitionsNonFinal(std::vector<AggregatedDataVariantsPtr> sources, size_t num_partitions) const
    {
        ManyAggregatedDataVariants many(sources.begin(), sources.end());
        std::atomic<bool> cancelled{false};
        auto prepared = aggregator.prepareVariantsToMerge(std::move(many), cancelled, /*adaptive_session=*/nullptr);

        size_t max_table_size = 0;
        for (const auto & variants : prepared)
            max_table_size = std::max(max_table_size, variants->sizeWithoutOverflowRow());
        std::map<String, String> united;
        for (size_t partition = 0; partition < num_partitions; ++partition)
        {
            auto agg_chunk = aggregator.mergeSingleLevelPartitionAndConvertToChunk(
                prepared, /*final=*/false, partition, num_partitions, max_table_size, cancelled, /*updater=*/nullptr);
            const size_t rows = agg_chunk.chunk.getNumRows();
            auto columns = agg_chunk.chunk.detachColumns();
            for (size_t i = params.keys_size; i < columns.size(); ++i)
                columns[i] = ColumnAggregateFunction::convertToValues(IColumn::mutate(std::move(columns[i])));
            Chunk finalized(std::move(columns), rows);
            for (const auto & [key, value] : renderChunk(finalized, params.keys_size))
            {
                EXPECT_TRUE(united.emplace(key, value).second) << "key " << key << " produced by two partitions";
            }
        }
        return united;
    }

    std::map<String, String> referenceOf(const std::vector<Columns> & blocks) const
    {
        auto variants = aggregate(blocks);
        std::map<String, String> result;
        auto chunks = aggregator.convertToChunks(*variants, /*final=*/true);
        for (const auto & aggregated : chunks)
            for (const auto & [key, value] : renderChunk(aggregated.chunk, params.keys_size))
                result[key] = value;
        return result;
    }
};

class AggregatorParallelPartitionMerge : public ::testing::Test
{
protected:
    void SetUp() override
    {
        MainThreadStatus::getInstance();
        tryRegisterAggregateFunctions();
    }
};

const DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
const DataTypePtr string_type = std::make_shared<DataTypeString>();

Block makeHeader(const DataTypePtr & key_type)
{
    return Block({{key_type->createColumn(), key_type, "k"}, {uint64_type->createColumn(), uint64_type, "v"}});
}

Columns makeUInt64Block(const std::vector<UInt64> & keys, UInt64 value_base = 0)
{
    std::vector<Field> key_fields(keys.begin(), keys.end());
    std::vector<Field> value_fields;
    value_fields.reserve(keys.size());
    for (auto key : keys)
        value_fields.emplace_back(key + value_base);
    return {makeColumn(uint64_type, key_fields), makeColumn(uint64_type, value_fields)};
}

}

/// The generic batch path (UInt64 key, sum): keys spread over partitions, split keys are combined, and
/// merging with a single partition degenerates to the whole result.
TEST_F(AggregatorParallelPartitionMerge, Key64Sum)
{
    Scenario scenario(makeHeader(uint64_type), makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    std::vector<UInt64> keys_a(200);
    std::vector<UInt64> keys_b(200);
    for (size_t i = 0; i < 200; ++i)
    {
        keys_a[i] = i;
        keys_b[i] = 100 + i;
    }
    std::vector<Columns> block_a{makeUInt64Block(keys_a)};
    std::vector<Columns> block_b{makeUInt64Block(keys_b)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(expected.size(), 300u);

    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 1), expected);
}

/// The inline count path (`is_simple_count`): split keys add their counts without any state objects.
TEST_F(AggregatorParallelPartitionMerge, SimpleCount)
{
    Scenario scenario(makeHeader(uint64_type), makeParams({"k"}, {makeAggregate("count", {}, {})}));

    std::vector<UInt64> keys_a(150);
    std::vector<UInt64> keys_b(150);
    for (size_t i = 0; i < 150; ++i)
    {
        keys_a[i] = i % 100;
        keys_b[i] = 50 + (i % 100);
    }
    std::vector<Columns> block_a{makeUInt64Block(keys_a)};
    std::vector<Columns> block_b{makeUInt64Block(keys_b)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// The string method: the partition of a key comes from the size-class dispatch hash
/// (`StringHashTable::hash`), which must equal the two-level bucketing.
TEST_F(AggregatorParallelPartitionMerge, StringKey)
{
    Scenario scenario(makeHeader(string_type), makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    auto make_block = [](size_t offset)
    {
        std::vector<Field> keys;
        std::vector<Field> values;
        for (size_t i = 0; i < 200; ++i)
        {
            /// Mixed lengths cover the short-string sub-tables and the long-string one.
            String key = "k" + std::to_string(offset + i);
            if (i % 3 == 0)
                key += "_long_enough_to_leave_the_packed_sub_tables";
            keys.emplace_back(key);
            values.emplace_back(UInt64(i));
        }
        return Columns{makeColumn(string_type, keys), makeColumn(uint64_type, values)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(100)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// The serialized multi-key method (UInt64 + String keys).
TEST_F(AggregatorParallelPartitionMerge, TwoKeySerialized)
{
    Block header(
        {{uint64_type->createColumn(), uint64_type, "a"},
         {string_type->createColumn(), string_type, "b"},
         {uint64_type->createColumn(), uint64_type, "v"}});
    Scenario scenario(header, makeParams({"a", "b"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    auto make_block = [](size_t offset)
    {
        std::vector<Field> a;
        std::vector<Field> b;
        std::vector<Field> v;
        for (size_t i = 0; i < 200; ++i)
        {
            a.emplace_back(UInt64((offset + i) % 150));
            b.emplace_back("s" + std::to_string((offset + i) % 90));
            v.emplace_back(UInt64(1));
        }
        return Columns{makeColumn(uint64_type, a), makeColumn(string_type, b), makeColumn(uint64_type, v)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(60)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// The packed two-UInt64 keys method (keys128).
TEST_F(AggregatorParallelPartitionMerge, Keys128)
{
    Block header(
        {{uint64_type->createColumn(), uint64_type, "a"},
         {uint64_type->createColumn(), uint64_type, "b"},
         {uint64_type->createColumn(), uint64_type, "v"}});
    Scenario scenario(header, makeParams({"a", "b"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    auto make_block = [](size_t offset)
    {
        std::vector<Field> a;
        std::vector<Field> b;
        std::vector<Field> v;
        for (size_t i = 0; i < 200; ++i)
        {
            a.emplace_back(UInt64((offset + i) % 130));
            b.emplace_back(UInt64((offset + i) % 7));
            v.emplace_back(UInt64(offset + i));
        }
        return Columns{makeColumn(uint64_type, a), makeColumn(uint64_type, b), makeColumn(uint64_type, v)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(70)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// The single nullable key: the NULL key lives in a dedicated slot outside the partitioned cells and
/// belongs to partition 0. NULL rows from several tables must combine into exactly one output row.
TEST_F(AggregatorParallelPartitionMerge, NullableKeyNullSlot)
{
    const DataTypePtr nullable_type = std::make_shared<DataTypeNullable>(uint64_type);
    Scenario scenario(makeHeader(nullable_type), makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    auto make_block = [&](size_t offset)
    {
        std::vector<Field> keys;
        std::vector<Field> values;
        for (size_t i = 0; i < 100; ++i)
        {
            if (i % 10 == 0)
                keys.emplace_back(Field());
            else
                keys.emplace_back(UInt64(offset + i));
            values.emplace_back(UInt64(1));
        }
        return Columns{makeColumn(nullable_type, keys), makeColumn(uint64_type, values)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(50)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    auto united = scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4);
    EXPECT_EQ(united, expected);
    EXPECT_EQ(united.at("NULL"), "20");
}

/// The low-cardinality string key method.
TEST_F(AggregatorParallelPartitionMerge, LowCardinalityStringKey)
{
    const DataTypePtr lc_type = std::make_shared<DataTypeLowCardinality>(string_type);
    Scenario scenario(makeHeader(lc_type), makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    auto make_block = [&](size_t offset)
    {
        std::vector<Field> keys;
        std::vector<Field> values;
        for (size_t i = 0; i < 120; ++i)
        {
            keys.emplace_back("lc" + std::to_string((offset + i) % 80));
            values.emplace_back(UInt64(i));
        }
        return Columns{makeColumn(lc_type, keys), makeColumn(uint64_type, values)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(40)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

TEST_F(AggregatorParallelPartitionMerge, LowCardinalityNormalizationSplitDistinctArenas)
{
    const DataTypePtr lc_type = std::make_shared<DataTypeLowCardinality>(uint64_type);
    auto params = makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})});
    params.max_threads = 4;
    params.group_by_two_level_threshold = 1;
    Scenario scenario(makeHeader(lc_type), params);

    /// Exceed the normal splitting threshold without a failpoint. The source's two shards
    /// retain the same aggregate-state arena but use separate arenas for normalization.
    constexpr size_t num_keys = 200'001;
    auto keys = ColumnUInt64::create(num_keys);
    auto indexes = ColumnUInt64::create(num_keys);
    for (size_t i = 0; i < num_keys; ++i)
        keys->getData()[i] = indexes->getData()[i] = i;
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*uint64_type, std::move(keys));
    auto key_column = ColumnLowCardinality::create(
        std::move(dictionary), std::move(indexes), /*is_shared=*/true, /*has_single_dictionary_for_part=*/true);
    auto source = scenario.aggregate({{std::move(key_column), ColumnUInt64::create(num_keys, 1)}});
    ASSERT_EQ(source->type, AggregatedDataVariants::Type::low_cardinality_single_dictionary_two_level);

    /// A value-key variant forces normalization before the final merge.
    auto other = scenario.aggregate({{makeColumn(lc_type, {UInt64(1)}), makeColumn(uint64_type, {UInt64(1)})}});
    ASSERT_FALSE(other->isSingleLowCardinalityDictionary());
    ManyAggregatedDataVariants sources{source, other};
    std::atomic<bool> cancelled{false};
    auto prepared = scenario.aggregator.prepareVariantsToMerge(std::move(sources), cancelled, /*adaptive_session=*/nullptr);
    ASSERT_EQ(prepared.size(), 3u);

    /// Check the worker-arena invariant directly instead of relying on a race to corrupt states.
    /// Concatenating the sibling lists would assign the same arena to workers 0 and 2.
    const auto & pools = prepared.front()->aggregates_pools;
    ASSERT_GE(pools.size(), params.max_threads);
    std::unordered_set<Arena *> unique_pools;
    for (const auto & pool : pools)
    {
        ASSERT_TRUE(pool);
        EXPECT_TRUE(unique_pools.insert(pool.get()).second);
    }

    size_t rows = 0;
    for (const auto & data : prepared)
    {
        EXPECT_TRUE(data->isTwoLevel());
        EXPECT_TRUE(unique_pools.contains(data->aggregates_pool));
        for (const auto & pool : data->aggregates_pools)
            EXPECT_TRUE(unique_pools.contains(pool.get()));
        rows += data->sizeWithoutOverflowRow();
    }
    EXPECT_EQ(rows, num_keys + 1);
}

TEST_F(AggregatorParallelPartitionMerge, LowCardinalityNormalizationPreservesFloatEncodings)
{
    auto check = []<typename Float, typename Bits>(bool nullable, bool two_level, size_t batch_size)
    {
        DataTypePtr dictionary_type = std::make_shared<DataTypeNumber<Float>>();
        if (nullable)
            dictionary_type = std::make_shared<DataTypeNullable>(dictionary_type);
        const DataTypePtr lc_type = std::make_shared<DataTypeLowCardinality>(dictionary_type);
        SCOPED_TRACE(::testing::Message() << lc_type->getName() << ", two_level=" << two_level << ", batch_size=" << batch_size);

        auto params = makeParams({"k"}, {makeAggregate("groupArray", {"v"}, {uint64_type})});
        params.max_block_size = batch_size;
        Scenario scenario(makeHeader(lc_type), std::move(params));

        /// Construct a legacy dictionary directly: ordinary insertion canonicalizes `NaN` values.
        /// Include signed zero and, for nullable keys, both the `NULL` and default-value slots.
        const Bits nan = std::bit_cast<Bits>(std::numeric_limits<Float>::quiet_NaN());
        std::vector<Float> values{Float{0}, -Float{0}, std::bit_cast<Float>(Bits(nan | 1)), std::bit_cast<Float>(Bits(nan | 2)), Float{1}};
        if (nullable)
            values.insert(values.begin(), Float{0});
        auto keys = ColumnVector<Float>::create();
        keys->getData().assign(values.begin(), values.end());
        MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys));
        auto indexes = ColumnUInt8::create();
        for (size_t i = 0; i < values.size(); ++i)
        {
            indexes->getData().push_back(static_cast<UInt8>(i));
            indexes->getData().push_back(static_cast<UInt8>(i));
        }
        auto key_column = ColumnLowCardinality::create(
            std::move(dictionary), std::move(indexes), /*is_shared=*/true, /*has_single_dictionary_for_part=*/true);
        auto source = scenario.aggregate({{std::move(key_column), makeColumn(uint64_type, std::vector<Field>(2 * values.size(), UInt64(1)))}});
        ASSERT_EQ(source->type, AggregatedDataVariants::Type::low_cardinality_single_dictionary);

        std::vector<AggregateDataPtr> states;
        for (size_t i = 0; i < values.size(); ++i)
        {
            auto * cell = source->low_cardinality_single_dictionary->data.find(i);
            ASSERT_TRUE(cell);
            states.push_back(cell->getMapped());
        }
        if (two_level)
            source->convertToTwoLevel();

        /// A value-key variant forces normalization of the index-key source before merging.
        auto other = scenario.aggregate({{makeColumn(lc_type, {Float64(1)}), makeColumn(uint64_type, {UInt64(1)})}});
        ASSERT_FALSE(other->isSingleLowCardinalityDictionary());
        ManyAggregatedDataVariants sources{source, other};
        std::atomic<bool> cancelled{false};
        auto prepared = scenario.aggregator.prepareVariantsToMerge(std::move(sources), cancelled, /*adaptive_session=*/nullptr);
        ASSERT_EQ(prepared.size(), 2u);

        auto check_table = [&](const auto & method)
        {
            ASSERT_TRUE(method);
            EXPECT_EQ(method->data.size(), values.size());
            EXPECT_EQ(method->data.hasNullKeyData(), nullable);
            for (size_t i = 0; i < values.size(); ++i)
            {
                if (nullable && i == 0)
                    EXPECT_EQ(method->data.getNullKeyData(), states[i]);
                else
                {
                    auto * cell = method->data.find(std::bit_cast<Bits>(values[i]));
                    ASSERT_TRUE(cell);
                    EXPECT_EQ(cell->getMapped(), states[i]);
                }
            }
        };

        if constexpr (std::is_same_v<Float, Float32>)
        {
            if (two_level)
                check_table(source->low_cardinality_key32_two_level);
            else
                check_table(source->low_cardinality_key32);
        }
        else
        {
            if (two_level)
                check_table(source->low_cardinality_key64_two_level);
            else
                check_table(source->low_cardinality_key64);
        }
    };

    for (bool nullable : {false, true})
        for (bool two_level : {false, true})
            for (size_t batch_size : {1u, 2u, 64u})
            {
                check.template operator()<Float32, UInt32>(nullable, two_level, batch_size);
                check.template operator()<Float64, UInt64>(nullable, two_level, batch_size);
            }
}

/// Heavy per-key states (`uniqExact`): first-seen keys adopt the state pointer, split keys go
/// through the batched merge of the state sets.
TEST_F(AggregatorParallelPartitionMerge, UniqExactHeavyStates)
{
    Scenario scenario(makeHeader(uint64_type), makeParams({"k"}, {makeAggregate("uniqExact", {"v"}, {uint64_type})}));

    auto make_block = [](size_t offset)
    {
        std::vector<UInt64> keys;
        keys.reserve(400);
        for (size_t i = 0; i < 400; ++i)
            keys.push_back((offset + i) % 60);
        return makeUInt64Block(keys, offset * 1000);
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(30)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// The non-final merge emits aggregate state columns instead of values. The states — both the
/// batch-merged cheap ones and the adopted heavy ones — must survive in the chunk (which owns the
/// source arenas) and finalize to exactly what the final merge produces.
TEST_F(AggregatorParallelPartitionMerge, NonFinalMergeProducesAdoptedStates)
{
    Scenario scenario(
        makeHeader(uint64_type),
        makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type}), makeAggregate("uniqExact", {"v"}, {uint64_type})}));

    auto make_block = [](size_t offset)
    {
        std::vector<UInt64> keys;
        keys.reserve(300);
        for (size_t i = 0; i < 300; ++i)
            keys.push_back((offset + i) % 80);
        return makeUInt64Block(keys, offset * 1000);
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(40)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitionsNonFinal({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// `GROUP BY` without aggregate functions uses the set (void-mapped) methods: the cells hold no state,
/// so the merge of a partition is a plain key union.
TEST_F(AggregatorParallelPartitionMerge, Key64Void)
{
    Scenario scenario(makeHeader(uint64_type), makeParams({"k"}, {}));

    std::vector<UInt64> keys_a(200);
    std::vector<UInt64> keys_b(200);
    for (size_t i = 0; i < 200; ++i)
    {
        keys_a[i] = i;
        keys_b[i] = 100 + i;
    }
    std::vector<Columns> block_a{makeUInt64Block(keys_a)};
    std::vector<Columns> block_b{makeUInt64Block(keys_b)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(expected.size(), 300u);

    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 1), expected);
}

/// A set method with a single nullable key: the NULL group carries no state either, so only its
/// presence in the dedicated slot of partition 0 travels from the sources to the result.
TEST_F(AggregatorParallelPartitionMerge, NullableKeyVoidNullSlot)
{
    const DataTypePtr nullable_type = std::make_shared<DataTypeNullable>(uint64_type);
    Scenario scenario(makeHeader(nullable_type), makeParams({"k"}, {}));

    auto make_block = [&](size_t offset)
    {
        std::vector<Field> keys;
        std::vector<Field> values;
        for (size_t i = 0; i < 100; ++i)
        {
            if (i % 10 == 0)
                keys.emplace_back(Field());
            else
                keys.emplace_back(UInt64(offset + i));
            values.emplace_back(UInt64(1));
        }
        return Columns{makeColumn(nullable_type, keys), makeColumn(uint64_type, values)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(50)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    auto united = scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4);
    EXPECT_EQ(united, expected);
    EXPECT_TRUE(united.contains("NULL"));
}

/// The serialized set method (`serialized_void`), whose keys live in the arena rather than in the cells.
TEST_F(AggregatorParallelPartitionMerge, SerializedVoid)
{
    Block header(
        {{uint64_type->createColumn(), uint64_type, "a"},
         {string_type->createColumn(), string_type, "b"},
         {uint64_type->createColumn(), uint64_type, "v"}});
    Scenario scenario(header, makeParams({"a", "b"}, {}));

    auto make_block = [](size_t offset)
    {
        std::vector<Field> a;
        std::vector<Field> b;
        std::vector<Field> v;
        for (size_t i = 0; i < 200; ++i)
        {
            a.emplace_back(UInt64((offset + i) % 150));
            /// Mixed lengths, so the serialized keys are not all of the same size.
            String key = "s" + std::to_string((offset + i) % 90);
            if (i % 3 == 0)
                key += "_long_enough_to_span_several_cells";
            b.emplace_back(key);
            v.emplace_back(UInt64(1));
        }
        return Columns{makeColumn(uint64_type, a), makeColumn(string_type, b), makeColumn(uint64_type, v)};
    };

    std::vector<Columns> block_a{make_block(0)};
    std::vector<Columns> block_b{make_block(60)};

    auto expected = scenario.referenceOf({block_a.front(), block_b.front()});
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block_a), scenario.aggregate(block_b)}, 4), expected);
}

/// A source that received no rows is filtered out by `prepareVariantsToMerge` and must not disturb
/// the partition merge of the remaining tables.
TEST_F(AggregatorParallelPartitionMerge, EmptySourceAmongTables)
{
    Scenario scenario(makeHeader(uint64_type), makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    std::vector<UInt64> keys(100);
    for (size_t i = 0; i < 100; ++i)
        keys[i] = i;
    std::vector<Columns> block{makeUInt64Block(keys)};

    auto expected = scenario.referenceOf({block.front()});
    auto empty = std::make_shared<AggregatedDataVariants>();
    EXPECT_EQ(scenario.mergePartitions({scenario.aggregate(block), empty}, 4), expected);
}

/// An adaptive producer can finish with an initialized two-level table but no rows. Its type
/// still promotes the live table to two-level, so a non-final merge must first normalize both
/// methods to value keys whose bucket numbers are stable outside this aggregation step.
TEST_F(AggregatorParallelPartitionMerge, InitializedEmptyTwoLevelSingleDictionarySource)
{
    const DataTypePtr lc_type = std::make_shared<DataTypeLowCardinality>(string_type);
    Scenario scenario(makeHeader(lc_type), makeParams({"k"}, {makeAggregate("sum", {"v"}, {uint64_type})}));

    auto key_column = lc_type->createColumn();
    auto value_column = uint64_type->createColumn();
    for (size_t i = 0; i < 16; ++i)
    {
        key_column->insert("value_" + std::to_string(i));
        value_column->insert(UInt64(1));
    }
    assert_cast<ColumnLowCardinality &>(*key_column).setHasSingleDictionaryForPart(true);

    auto live = scenario.aggregate({Columns{std::move(key_column), std::move(value_column)}});
    ASSERT_EQ(live->type, AggregatedDataVariants::Type::low_cardinality_single_dictionary);

    auto initialized_empty = std::make_shared<AggregatedDataVariants>();
    initialized_empty->init(AggregatedDataVariants::Type::low_cardinality_single_dictionary);
    initialized_empty->convertToTwoLevel();

    ManyAggregatedDataVariants sources{live, initialized_empty};
    std::atomic<bool> cancelled{false};
    auto prepared = scenario.aggregator.prepareVariantsToMerge(
        std::move(sources), cancelled, /*adaptive_session=*/nullptr, /*require_stable_bucket_hash=*/true);

    ASSERT_EQ(prepared.size(), 2u);
    for (const auto & variants : prepared)
    {
        EXPECT_EQ(variants->type, AggregatedDataVariants::Type::low_cardinality_key_string_two_level);
        EXPECT_FALSE(variants->isSingleLowCardinalityDictionary());
    }
}
