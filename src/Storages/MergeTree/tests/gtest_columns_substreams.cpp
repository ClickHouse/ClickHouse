#include <gtest/gtest.h>

#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

/// A count above the address space fails to parse at the end of the file instead of throwing `std::bad_alloc`.
TEST(ColumnsSubstreams, ReadTextThrowsOnCountsBeyondFile)
{
    const std::vector<String> files = {
        "columns substreams version: 1\n1000000000000000 columns:\n",
        "columns substreams version: 1\n1 columns:\n1000000000000000 substreams for column `c`:\n\tc\n",
    };

    for (const auto & file : files)
    {
        SCOPED_TRACE(file);
        ReadBufferFromString buf(file);
        ColumnsSubstreams columns_substreams;
        EXPECT_THROW(columns_substreams.readText(buf), Exception);
    }
}

/// Entries the writer cannot produce: a column without substreams, a substream recorded twice.
TEST(ColumnsSubstreams, ReadTextThrowsOnInvalidColumnEntry)
{
    const std::vector<String> files = {
        "columns substreams version: 1\n1 columns:\n0 substreams for column `c`:\n",
        "columns substreams version: 1\n1 columns:\n2 substreams for column `c`:\n\tc\n\tc\n",
    };

    for (const auto & file : files)
    {
        SCOPED_TRACE(file);
        ReadBufferFromString buf(file);
        ColumnsSubstreams columns_substreams;
        try
        {
            columns_substreams.readText(buf);
            ADD_FAILURE() << "readText accepted an invalid column entry";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA);
        }
    }
}
