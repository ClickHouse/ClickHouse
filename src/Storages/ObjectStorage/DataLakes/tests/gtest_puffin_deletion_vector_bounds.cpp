#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

TEST(PuffinDeletionVectorBounds, RejectsLengthExceedingFileSize)
{
    const String data(64, '\0');
    ReadBufferFromOutsideMemoryFile file("test.puffin", data);

    try
    {
        readDeletionVectorFromPuffin(file, 0, 1000);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("offset/length out of bounds"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorBounds, RejectsOffsetPlusLengthOverflow)
{
    const String data(64, '\0');
    ReadBufferFromOutsideMemoryFile file("test.puffin", data);

    try
    {
        readDeletionVectorFromPuffin(file, 60, 12);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("offset/length out of bounds"), std::string::npos);
    }
}

TEST(PuffinDeletionVectorBounds, ValidatePuffinBlobBoundsRejectsNegativeOffset)
{
    try
    {
        validatePuffinBlobBounds(-1, 10, 64);
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
        EXPECT_NE(e.message().find("offset/length out of bounds"), std::string::npos);
    }
}
