#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <limits>

using namespace DB;

namespace
{

template <typename T>
T readVarUIntValue(UInt64 value)
{
    WriteBufferFromOwnString write_buffer;
    writeVarUInt(value, write_buffer);
    write_buffer.finalize();

    const auto & data = write_buffer.str();
    ReadBufferFromMemory read_buffer(data.data(), data.size());

    T result{};
    readVarUInt(result, read_buffer);
    return result;
}

template <typename T>
T readVarIntValue(Int64 value)
{
    WriteBufferFromOwnString write_buffer;
    writeVarInt(value, write_buffer);
    write_buffer.finalize();

    const auto & data = write_buffer.str();
    ReadBufferFromMemory read_buffer(data.data(), data.size());

    T result{};
    readVarInt(result, read_buffer);
    return result;
}

}

TEST(VarInt, VarUIntRejectsValuesOutsideTargetRange)
{
    EXPECT_THROW(
        readVarUIntValue<UInt8>(
            static_cast<UInt64>(std::numeric_limits<UInt8>::max()) + 1),
        Exception);

    EXPECT_THROW(
        readVarUIntValue<UInt16>(
            static_cast<UInt64>(std::numeric_limits<UInt16>::max()) + 1),
        Exception);

    EXPECT_THROW(
        readVarUIntValue<UInt32>(
            static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1),
        Exception);
}

TEST(VarInt, VarUIntAcceptsMaximumTargetValues)
{
    EXPECT_EQ(
        readVarUIntValue<UInt8>(std::numeric_limits<UInt8>::max()),
        std::numeric_limits<UInt8>::max());

    EXPECT_EQ(
        readVarUIntValue<UInt16>(std::numeric_limits<UInt16>::max()),
        std::numeric_limits<UInt16>::max());

    EXPECT_EQ(
        readVarUIntValue<UInt32>(std::numeric_limits<UInt32>::max()),
        std::numeric_limits<UInt32>::max());
}

TEST(VarInt, VarIntRejectsValuesOutsideInt16Range)
{
    EXPECT_THROW(
        readVarIntValue<Int16>(
            static_cast<Int64>(std::numeric_limits<Int16>::max()) + 1),
        Exception);

    EXPECT_THROW(
        readVarIntValue<Int16>(
            static_cast<Int64>(std::numeric_limits<Int16>::min()) - 1),
        Exception);
}

TEST(VarInt, VarIntRejectsValuesOutsideInt32Range)
{
    EXPECT_THROW(
        readVarIntValue<Int32>(
            static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1),
        Exception);

    EXPECT_THROW(
        readVarIntValue<Int32>(
            static_cast<Int64>(std::numeric_limits<Int32>::min()) - 1),
        Exception);
}

TEST(VarInt, VarIntAcceptsSignedBoundaryValues)
{
    EXPECT_EQ(
        readVarIntValue<Int16>(std::numeric_limits<Int16>::min()),
        std::numeric_limits<Int16>::min());

    EXPECT_EQ(
        readVarIntValue<Int16>(std::numeric_limits<Int16>::max()),
        std::numeric_limits<Int16>::max());

    EXPECT_EQ(
        readVarIntValue<Int32>(std::numeric_limits<Int32>::min()),
        std::numeric_limits<Int32>::min());

    EXPECT_EQ(
        readVarIntValue<Int32>(std::numeric_limits<Int32>::max()),
        std::numeric_limits<Int32>::max());
}

TEST(VarInt, UInt64ReadIsUnchanged)
{
    constexpr UInt64 value = 0x123456789ABCDEF0ULL;

    EXPECT_EQ(readVarUIntValue<UInt64>(value), value);
}
