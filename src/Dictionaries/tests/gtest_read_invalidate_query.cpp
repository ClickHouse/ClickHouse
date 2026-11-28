#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/Block.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB::ErrorCodes
{
    extern const int RECEIVED_EMPTY_DATA;
}

using namespace DB;

/// A result of one row that carries no columns is not empty, and it has no column to take the value from:
/// the query fails with an exception instead of reading the first column of a block that has none.
TEST(ReadInvalidateQuery, RowWithoutColumnsIsRefused)
{
    auto header = std::make_shared<const Block>();
    QueryPipeline pipeline(Pipe(std::make_shared<SourceFromSingleChunk>(header, Chunk(Columns{}, 1))));

    try
    {
        readInvalidateQuery(pipeline);
        FAIL() << "Expected RECEIVED_EMPTY_DATA";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::RECEIVED_EMPTY_DATA);
    }
}
