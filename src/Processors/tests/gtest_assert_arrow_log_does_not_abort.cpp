#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <gtest/gtest.h>
#include <arrow/chunked_array.h>
#include <vector>
#include <arrow/util/logging.h>

namespace DB
{

TEST(ChunkedArray, ChunkedArrayWithZeroChunksShouldNotAbort)
{
    std::vector<std::shared_ptr<::arrow::Array>> empty_chunks_vector;

    EXPECT_ANY_THROW(::arrow::ChunkedArray{empty_chunks_vector});
}

TEST(ArrowLog, FatalLogShouldThrow)
{
    EXPECT_ANY_THROW(::arrow::util::ArrowLog(__FILE__, __LINE__, ::arrow::util::ArrowLogLevel::ARROW_FATAL));
}

}

#endif
