#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>

// I know that inclusion of .cpp is not good at all
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.cpp> // NOLINT

using namespace DB;

TEST(IndexGranularityCompactParts, FillGranularitySequenceOfBlocks)
{
    { /// Three blocks in one granule.
        size_t rows = 8;
        size_t granularity = 32;

        MergeTreeIndexGranularity index_granularity;
        size_t index_offset = 0;
        size_t rows_written = 0;
        for (size_t i = 0; i < 3; ++i)
        {
            fillIndexGranularityImpl(index_granularity, index_offset, granularity, rows);
            rows_written += rows;
            index_offset = granularity - rows_written;
        }

        EXPECT_EQ(index_granularity.getMarksCount(), 1); /// granules
        /// It's ok, that granularity is higher than actual number of row.
        /// It will be corrected in CompactWriter.
        EXPECT_EQ(index_granularity.getMarkRows(0), granularity);
    }

    { /// Granule is extended with small block
        size_t rows1 = 30;
        size_t rows2 = 8;
        size_t granularity = 32;

        MergeTreeIndexGranularity index_granularity;
        size_t index_offset = 0;

        fillIndexGranularityImpl(index_granularity, index_offset, granularity, rows1);
        index_offset = granularity - rows1;

        fillIndexGranularityImpl(index_granularity, index_offset, granularity, rows2);

        EXPECT_EQ(index_granularity.getMarksCount(), 1);
        EXPECT_EQ(index_granularity.getMarkRows(0), rows1 + rows2);
    }

    { /// New granule is created with large block;
        size_t rows1 = 30;
        size_t rows2 = 25;
        size_t granularity = 32;

        MergeTreeIndexGranularity index_granularity;
        size_t index_offset = 0;

        fillIndexGranularityImpl(index_granularity, index_offset, granularity, rows1);
        index_offset = granularity - rows1;

        fillIndexGranularityImpl(index_granularity, index_offset, granularity, rows2);

        EXPECT_EQ(index_granularity.getMarksCount(), 2);
        EXPECT_EQ(index_granularity.getMarkRows(0), granularity);
        EXPECT_EQ(index_granularity.getMarkRows(1), rows1 + rows2 - granularity);
    }

     { /// Three large blocks
        size_t rows = 40;
        size_t granularity = 32;

        MergeTreeIndexGranularity index_granularity;
        size_t index_offset = 0;

        for (size_t i = 0; i < 3; ++i)
            fillIndexGranularityImpl(index_granularity, index_offset, granularity, rows);

        EXPECT_EQ(index_granularity.getMarksCount(), 3);
        for (size_t i = 0; i < 3; ++i)
            EXPECT_EQ(index_granularity.getMarkRows(i), rows);
    }
}
