#include "config.h"

#if USE_JEMALLOC

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemJemallocSampledAllocations.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

namespace
{

struct SampledAllocation
{
    Array trace;
    std::vector<UInt64> fields;
};

std::vector<SampledAllocation> readSampledAllocations(std::string_view profile, const std::string & name)
{
    auto path = (std::filesystem::temp_directory_path() / name).string();
    {
        std::ofstream out(path);
        out.write(profile.data(), profile.size());
    }

    Block header;
    for (const auto & column : StorageSystemJemallocSampledAllocations::getColumnsDescription().getOrdinary())
        header.insert({column.type->createColumn(), column.type, column.name});

    QueryPipeline pipeline(StorageSystemJemallocSampledAllocations::readHeapProfile(
        path, std::make_shared<const Block>(std::move(header)), DEFAULT_BLOCK_SIZE));
    PullingPipelineExecutor executor(pipeline);

    std::vector<SampledAllocation> rows;
    Block block;
    while (executor.pull(block))
    {
        for (size_t row = 0; row < block.rows(); ++row)
        {
            SampledAllocation allocation;
            Field trace;
            block.getByPosition(0).column->get(row, trace);
            allocation.trace = trace.safeGet<Array>();
            for (size_t col = 1; col < block.columns(); ++col)
                allocation.fields.push_back(block.getByPosition(col).column->getUInt(row));
            rows.push_back(std::move(allocation));
        }
    }
    return rows;
}

}

TEST(JemallocSampledAllocations, AllocationWithEmptyBacktrace)
{
    /// jemalloc writes a bare `@` when it could not unwind the allocation.
    constexpr std::string_view profile = R"(heap_v2/524288
  t*: 3: 3136 [0: 0]
  t1: 3: 3136 [0: 0]
@
  t*: 1: 1024 [0: 0]
  t1: 1: 1024 [0: 0]
  f: 2000 1000 1024 15 1 1
@ 0x1e51c218 0x1e51c118
  t*: 1: 64 [0: 0]
  t1: 1: 64 [0: 0]
  f: 1000 60 64 4 0 1
@
  t*: 1: 2048 [0: 0]
  t1: 1: 2048 [0: 0]
  f: 3000 1500 2048 16 2 1
)";

    auto rows = readSampledAllocations(profile, "gtest_jemalloc_sampled_allocations_empty_backtrace.heap");
    ASSERT_EQ(rows.size(), 3);

    EXPECT_EQ(rows[0].trace, Array{});
    EXPECT_EQ(rows[0].fields, (std::vector<UInt64>{2000, 1000, 1024, 15, 1, 1, 524288}));

    EXPECT_EQ(rows[1].trace, (Array{UInt64{0x1e51c218}, UInt64{0x1e51c117}}));
    EXPECT_EQ(rows[1].fields, (std::vector<UInt64>{1000, 60, 64, 4, 0, 1, 524288}));

    EXPECT_EQ(rows[2].trace, Array{});
    EXPECT_EQ(rows[2].fields, (std::vector<UInt64>{3000, 1500, 2048, 16, 2, 1, 524288}));
}

TEST(JemallocSampledAllocations, AllocationRecordBeforeAnyBacktrace)
{
    constexpr std::string_view profile = R"(heap_v2/524288
  t*: 1: 64 [0: 0]
  f: 1000 60 64 4 0 1
)";

    try
    {
        readSampledAllocations(profile, "gtest_jemalloc_sampled_allocations_no_backtrace.heap");
        FAIL() << "Expected CANNOT_PARSE_TEXT";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CANNOT_PARSE_TEXT) << e.message();
    }
}

#endif
