#include <gtest/gtest.h>

#include <limits>

#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <base/types.h>

using namespace Paimon;

namespace
{

/// Builds a little-endian Paimon BinaryRow blob holding a single non-null fixed-size field.
/// Layout: 4-byte big-endian arity, then the null bitset, then the value slot.
/// slot_bytes controls how many bytes back the value slot. Paimon uses 8-byte slots; passing
/// a smaller value produces a deliberately short buffer so a read wider than slot_bytes runs
/// past the end and throws (used by GetIntReadsExactly32Bits to observe the read width).
String makeSingleFieldRow(UInt64 raw_value, size_t slot_bytes = 8)
{
    constexpr Int32 arity = 1;
    /// ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8 == 8 for arity == 1.
    constexpr Int32 bit_set_width = 8;

    String bytes;
    /// arity is stored big-endian.
    bytes.push_back(static_cast<char>((arity >> 24) & 0xFF));
    bytes.push_back(static_cast<char>((arity >> 16) & 0xFF));
    bytes.push_back(static_cast<char>((arity >> 8) & 0xFF));
    bytes.push_back(static_cast<char>(arity & 0xFF));

    /// Null bitset: all zeros -> field 0 is not null.
    bytes.append(bit_set_width, '\0');

    /// Value slot, little-endian.
    for (size_t i = 0; i < slot_bytes; ++i)
        bytes.push_back(static_cast<char>((raw_value >> (i * 8)) & 0xFF));

    return bytes;
}

}

/// Paimon stores every fixed-size field in an 8-byte slot, so getLong must read all 64 bits.
/// Reading only the low 32 bits truncated large BIGINT partition values (issue #109477):
/// e.g. 9223372036854775807 became -1.
TEST(PaimonBinaryRow, GetLongReadsFull64Bits)
{
    {
        BinaryRow row(makeSingleFieldRow(1000));
        EXPECT_EQ(row.getLong(0), 1000);
    }
    {
        /// Value beyond the Int32 range must not truncate.
        BinaryRow row(makeSingleFieldRow(5000000000ULL));
        EXPECT_EQ(row.getLong(0), 5000000000LL);
    }
    {
        BinaryRow row(makeSingleFieldRow(static_cast<UInt64>(std::numeric_limits<Int64>::max())));
        EXPECT_EQ(row.getLong(0), std::numeric_limits<Int64>::max());
    }
    {
        BinaryRow row(makeSingleFieldRow(static_cast<UInt64>(std::numeric_limits<Int64>::min())));
        EXPECT_EQ(row.getLong(0), std::numeric_limits<Int64>::min());
    }
    {
        BinaryRow row(makeSingleFieldRow(static_cast<UInt64>(-1LL)));
        EXPECT_EQ(row.getLong(0), -1);
    }
}

/// getInt must keep reading exactly 32 bits (regression guard for the sibling getter).
///
/// Observing only the returned Int32 cannot distinguish a 32-bit read from a 64-bit read
/// narrowed back to Int32 (both keep the low 32 bits). So the width is observed structurally
/// instead: the value slot is backed by exactly 4 bytes, making the buffer too short for an
/// 8-byte read. getFixedSizeData<Int32> reads all 4 bytes and succeeds; a mistaken widening to
/// getFixedSizeData<Int64> would read 8 bytes, run past the end, and throw. The correct value
/// is also checked to guard against a narrower-than-32-bit read.
TEST(PaimonBinaryRow, GetIntReadsExactly32Bits)
{
    BinaryRow row(makeSingleFieldRow(0x89ABCDEFULL, /*slot_bytes=*/4));
    EXPECT_EQ(row.getInt(0), static_cast<Int32>(0x89ABCDEF));
}
