#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVector.h>

#include <vector>

using namespace DB;
using namespace DB::Iceberg;

namespace DB
{
namespace ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}
}

TEST(IcebergDeletionVectorPositions, AcceptsBoundaryPosition)
{
    /// Valid file-local positions are in [0, record_count).
    const std::vector<UInt64> positions = {0, 9};
    EXPECT_NO_THROW(validateDeletionVectorPositionsAgainstDataFile(positions, /*expected_cardinality=*/2, /*data_file_record_count=*/10));
}

TEST(IcebergDeletionVectorPositions, RejectsPositionEqualToRecordCount)
{
    const std::vector<UInt64> positions = {10};
    try
    {
        validateDeletionVectorPositionsAgainstDataFile(positions, /*expected_cardinality=*/1, /*data_file_record_count=*/10);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
        EXPECT_NE(e.message().find("out of range"), std::string::npos);
    }
}

TEST(IcebergDeletionVectorPositions, RejectsPositionAboveRecordCount)
{
    const std::vector<UInt64> positions = {11};
    try
    {
        validateDeletionVectorPositionsAgainstDataFile(positions, /*expected_cardinality=*/1, /*data_file_record_count=*/10);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
        EXPECT_NE(e.message().find("out of range"), std::string::npos);
    }
}

TEST(IcebergDeletionVectorPositions, RejectsCardinalityExceedingRecordCount)
{
    const std::vector<UInt64> positions;
    try
    {
        validateDeletionVectorPositionsAgainstDataFile(positions, /*expected_cardinality=*/11, /*data_file_record_count=*/10);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
        EXPECT_NE(e.message().find("exceeds data file record_count"), std::string::npos);
    }
}

TEST(IcebergDeletionVectorPositions, RejectsNegativeDataFileRecordCount)
{
    const std::vector<UInt64> positions;
    try
    {
        validateDeletionVectorPositionsAgainstDataFile(positions, /*expected_cardinality=*/0, /*data_file_record_count=*/-1);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION);
        EXPECT_NE(e.message().find("non-negative"), std::string::npos);
    }
}

#endif
