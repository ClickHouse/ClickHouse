#include <gtest/gtest.h>

#include <map>
#include <vector>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Aggregator.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ThreadStatus.h>
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
        auto prepared = aggregator.prepareVariantsToMerge(std::move(many), /*adaptive_session=*/nullptr);
        EXPECT_FALSE(prepared.empty());
        EXPECT_FALSE(prepared.at(0)->isTwoLevel());
        EXPECT_TRUE(aggregator.canMergeSingleLevelInPartitions(*prepared.at(0)));

        std::atomic<bool> cancelled{false};
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
        auto prepared = aggregator.prepareVariantsToMerge(std::move(many), /*adaptive_session=*/nullptr);

        std::atomic<bool> cancelled{false};
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
