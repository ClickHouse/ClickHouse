#include <gtest/gtest.h>

#include <numeric>
#include <thread>
#include <tuple>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

using namespace DB;

namespace
{

Chunk makeChunk(const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (auto value : values)
        column->insertValue(value);

    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), values.size());
}

/// Runs the chunks through a `DistinctTransform` and returns the total number of the output rows.
size_t runDistinct(SharedHeader header, Chunks chunks, bool allow_abandoning, bool skip_null_keys)
{
    auto source = std::make_shared<SourceFromChunks>(header, std::move(chunks));
    auto transform = std::make_shared<DistinctTransform>(
        header, SizeLimits{}, /*limit_hint_=*/ 0, Names{}, allow_abandoning, skip_null_keys);

    connect(source->getPort(), transform->getInputs().front());

    auto * output_port = &transform->getOutputs().front();
    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(source));
    processors->emplace_back(std::move(transform));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    PullingPipelineExecutor executor(pipeline);

    size_t rows = 0;
    Block block;
    while (executor.pull(block))
        rows += block.rows();
    return rows;
}

}

TEST(DistinctTransformAbandon, AbandonsOnMostlyUniqueInput)
{
    const auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});

    /// Five fully unique chunks fill the observation window with a unique rate of 1, then a fully
    /// duplicate chunk follows: with abandoning allowed the transform has dropped its set by then and
    /// the duplicates pass through; with it disallowed they are removed.
    auto make_chunks = []
    {
        Chunks chunks;
        for (size_t i = 0; i < 5; ++i)
        {
            std::vector<UInt64> values(100);
            std::iota(values.begin(), values.end(), i * 100);
            chunks.push_back(makeChunk(values));
        }

        std::vector<UInt64> duplicates(100);
        std::iota(duplicates.begin(), duplicates.end(), 0);
        chunks.push_back(makeChunk(duplicates));
        return chunks;
    };

    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ true, /*skip_null_keys=*/ false), 600u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 500u);
}

TEST(DistinctTransformAbandon, KeepsDeduplicatingDuplicateHeavyInput)
{
    const auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")});

    /// Every chunk repeats the same values: the unique rate stays near zero, so the transform must
    /// keep deduplicating even with abandoning allowed.
    auto make_chunks = []
    {
        Chunks chunks;
        for (size_t i = 0; i < 8; ++i)
        {
            std::vector<UInt64> values(100);
            std::iota(values.begin(), values.end(), 0);
            chunks.push_back(makeChunk(values));
        }
        return chunks;
    };

    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ true, /*skip_null_keys=*/ false), 100u);
}

TEST(DistinctTransformSkipNullKeys, DropsNullKeyRows)
{
    const auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    const auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(type, "k")});

    auto make_chunks = [&]
    {
        auto column = type->createColumn();
        column->insert(Field(UInt64(1)));
        column->insertDefault();
        column->insert(Field(UInt64(2)));
        column->insert(Field(UInt64(1)));
        column->insertDefault();

        Columns columns;
        columns.emplace_back(std::move(column));
        Chunks chunks;
        chunks.push_back(Chunk(std::move(columns), 5));
        return chunks;
    };

    /// With the skipping the `NULL` rows are dropped entirely; without it `NULL` is one distinct value.
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ true), 2u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 3u);
}

TEST(DistinctTransformSkipNullKeys, ConstNullKeyEmitsNothing)
{
    const auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    const auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(type->createColumnConst(1, Field{}), type, "c")});

    auto make_chunks = [&]
    {
        auto column = type->createColumn();
        for (size_t i = 0; i < 3; ++i)
            column->insertDefault();

        Columns columns;
        columns.emplace_back(std::move(column));
        Chunks chunks;
        chunks.push_back(Chunk(std::move(columns), 3));
        return chunks;
    };

    /// A constant `NULL` key makes every key contain a `NULL`: with the skipping nothing is emitted, while
    /// without it the constant-columns-only special case returns a single row.
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ true), 0u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 1u);
}

TEST(DistinctTransformSkipNullKeys, DropsLowCardinalityNullableNullRows)
{
    const auto type = std::make_shared<DataTypeLowCardinality>(
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    const auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(type, "k")});

    auto make_chunks = [&]
    {
        auto column = type->createColumn();
        column->insertData("a", 1);
        column->insertDefault();
        column->insertData("b", 1);
        column->insertData("a", 1);
        column->insertDefault();

        Columns columns;
        columns.emplace_back(std::move(column));
        Chunks chunks;
        chunks.push_back(Chunk(std::move(columns), 5));
        return chunks;
    };

    /// The `NULL` rows of a `LowCardinality(Nullable)` key are the rows referencing the dictionary's
    /// `NULL` entry; with the skipping they are dropped entirely, without it `NULL` is one distinct value.
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ true), 2u);
    EXPECT_EQ(runDistinct(header, make_chunks(), /*allow_abandoning=*/ false, /*skip_null_keys=*/ false), 3u);
}

TEST(DistinctTransformMemory, PassThroughBeforeLowCardinalityBitmapAllocation)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        const auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(type, "k")});
        constexpr size_t dictionary_size = 200000;
        auto dictionary = type->createColumn();
        for (size_t i = 0; i < dictionary_size; ++i)
            dictionary->insert(Field(std::to_string(i)));
        auto column = dictionary->cut(0, 3);
        dictionary.reset();
        ASSERT_GE(assert_cast<const ColumnLowCardinality &>(*column).getDictionary().size(), dictionary_size);
        /// The hash table can hold the three keys, but the dictionary bitmap exceeds the remaining budget.
        const UInt64 threshold = query.get() + 128 * 1024;
        DistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{},
            /*allow_abandoning_=*/ false, /*skip_null_keys_=*/ false, threshold);
        for (size_t i = 0; i < 2; ++i)
        {
            Chunk input({column}, 3);
            ASSERT_NO_THROW(static_cast<ISimpleTransform &>(transform).transform(input));
            EXPECT_EQ(input.getNumRows(), 3);
        }
    }).join();
}

TEST(DistinctTransformMemory, FilteringWidePayloadsWithSpareTableCapacity)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
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
            for (const bool constrained : {false, true})
            {
                SCOPED_TRACE(::testing::Message() << payload->getName() << ", constrained=" << constrained);
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

                /// Materialization fits, but copying the seven selected payloads exceeds the budget.
                /// The eight numeric keys fit the initial hash-table capacity without growth.
                const UInt64 threshold = constrained
                    ? query.get() + materialization_bytes + filtering_bytes / 4 + 65536
                    : 1ULL << 30;
                DistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"},
                    /*allow_abandoning_=*/ false, /*skip_null_keys_=*/ false, threshold);
                ASSERT_NO_THROW(static_cast<ISimpleTransform &>(transform).transform(input));
                ASSERT_EQ(input.getNumRows(), constrained ? num_rows : num_rows - 1);
                EXPECT_EQ(input.getColumns()[1]->getDataAt(0).size(), 1 << 20);

                auto repeated = makeChunk({1, 2, 3, 4, 5, 6, 7, 7});
                repeated.addColumn(payload);
                input.clear();
                ASSERT_NO_THROW(static_cast<ISimpleTransform &>(transform).transform(repeated));
                EXPECT_EQ(repeated.getNumRows(), constrained ? num_rows : 0);
            }
        }
    }).join();
}

TEST(DistinctTransformMemory, PassThroughBeforeFilteringExceedsQueryThreshold)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto header = std::make_shared<const Block>(Block{
            ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
            ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "payload")});
        auto payload = ColumnString::create();
        for (size_t row = 0; row < 8; ++row)
            payload->insert(Field(String(1 << 20, 'x')));
        auto input = makeChunk({1, 2, 3, 4, 5, 6, 7, 7});
        input.addColumn(std::move(payload));

        /// Input fits below the threshold, but copying its seven selected rows would exceed it.
        const UInt64 threshold = query.get() + input.allocatedBytes() / 4;
        DistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{"k"},
            /*allow_abandoning_=*/ false, /*skip_null_keys_=*/ false, threshold);
        ASSERT_LT(query.get(), threshold);
        static_cast<ISimpleTransform &>(transform).transform(input);
        EXPECT_EQ(input.getNumRows(), 8);

        /// Pass-through remains active after the wide payload is released.
        input.clear();
        auto repeated = makeChunk({1, 1});
        auto empty_payload = ColumnString::create();
        empty_payload->insertManyDefaults(2);
        repeated.addColumn(std::move(empty_payload));
        static_cast<ISimpleTransform &>(transform).transform(repeated);
        EXPECT_EQ(repeated.getNumRows(), 2);
    }).join();
}

TEST(DistinctTransformMemory, PassThroughBeforePackingDuplicateKeys)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        for (const bool packed : {false, true})
        {
            SCOPED_TRACE(packed);
            Block block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};
            if (packed)
                block.insert(ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "other"));
            const auto header = std::make_shared<const Block>(std::move(block));
            constexpr UInt64 threshold = 1 << 20;
            DistinctTransform transform(header, SizeLimits{}, /*limit_hint_=*/ 0, Names{},
                /*allow_abandoning_=*/ false, /*skip_null_keys_=*/ false, threshold);
            std::vector<UInt64> keys(2049);
            std::iota(keys.begin(), keys.end(), 0);
            auto first = makeChunk(keys);
            if (packed)
                first.addColumn(ColumnUInt8::create(keys.size(), UInt8{1}));
            static_cast<ISimpleTransform &>(transform).transform(first);
            ASSERT_EQ(first.getNumRows(), keys.size());
            first.clear();

            constexpr size_t rows = 4096;
            Chunk duplicates(Columns{ColumnUInt64::create(rows, UInt64{0})}, rows);
            if (packed)
                duplicates.addColumn(ColumnUInt8::create(rows, UInt8{1}));

            /// The table has room for another chunk, and masks and filtering copies fit in 64 KiB.
            /// Packing the duplicate composite keys needs a 128 KiB buffer before any lookup occurs.
            const Int64 pressure = threshold - query.get() - (64 << 10);
            ASSERT_GT(pressure, 0);
            std::ignore = CurrentMemoryTracker::alloc(pressure);
            SCOPE_EXIT(std::ignore = CurrentMemoryTracker::free(pressure));
            query.setHardLimit(threshold);
            SCOPE_EXIT(query.setHardLimit(0));
            ASSERT_NO_THROW(static_cast<ISimpleTransform &>(transform).transform(duplicates));
            EXPECT_EQ(duplicates.getNumRows(), packed ? rows : 0);
        }
    }).join();
}
