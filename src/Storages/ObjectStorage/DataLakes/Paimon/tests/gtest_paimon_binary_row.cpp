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

/// Builds a Paimon BinaryRow blob holding a single non-null non-compact decimal field
/// (precision > 18). Such a decimal is stored out of line: the 8-byte slot holds
/// (offset << 32) | size, and the variable-length area holds the unscaled value as a big-endian
/// two's complement byte array of minimal length, as produced by Java `BigInteger.toByteArray`.
String makeSingleWideDecimalRow(const String & unscaled_big_endian)
{
    constexpr Int32 bit_set_width = 8;
    /// The variable-length area starts right after the null bitset and the single value slot.
    constexpr UInt64 sub_offset = bit_set_width + 8;
    const UInt64 offset_and_size = (sub_offset << 32) | unscaled_big_endian.size();

    String bytes = makeSingleFieldRow(offset_and_size);
    bytes += unscaled_big_endian;
    /// Paimon pads the variable-length data to a multiple of 8 bytes.
    bytes.append((8 - unscaled_big_endian.size() % 8) % 8, '\0');
    return bytes;
}

/// Minimal big-endian two's complement representation of `value`, like Java `BigInteger.toByteArray`.
template <typename T>
String toMinimalBigEndian(T value)
{
    String bytes;
    for (size_t i = 0; i < sizeof(T); ++i)
        bytes.insert(bytes.begin(), static_cast<char>(static_cast<UInt8>(value >> (i * 8))));
    /// Drop redundant leading sign bytes while keeping the sign bit of the next byte intact.
    while (bytes.size() > 1)
    {
        const UInt8 first = static_cast<UInt8>(bytes[0]);
        const UInt8 second = static_cast<UInt8>(bytes[1]);
        if ((first == 0x00 && !(second & 0x80)) || (first == 0xFF && (second & 0x80)))
            bytes.erase(0, 1);
        else
            break;
    }
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

/// Wide decimals (precision > 18) are stored as minimal big-endian two's complement byte arrays.
/// When the array is shorter than the target integer, it must be sign-extended: zero padding
/// turned negative values into large positive ones (issue #127544).
TEST(PaimonBinaryRow, GetDecimalSignExtendsWideValues)
{
    for (Int128 value : {Int128(0), Int128(1), Int128(-1), Int128(127), Int128(128), Int128(-128), Int128(-129),
                         Int128(-12345678901234567LL) * 1000, std::numeric_limits<Int128>::min(), std::numeric_limits<Int128>::max()})
    {
        BinaryRow row(makeSingleWideDecimalRow(toMinimalBigEndian(value)));
        EXPECT_EQ(row.getDecimal<Int128>(0, 38, 2).value, value);
    }

    for (Int256 value : {Int256(0), Int256(-1), Int256(255), Int256(-256),
                         Int256(std::numeric_limits<Int128>::min()) * 1000, std::numeric_limits<Int256>::min(), std::numeric_limits<Int256>::max()})
    {
        BinaryRow row(makeSingleWideDecimalRow(toMinimalBigEndian(value)));
        EXPECT_EQ(row.getDecimal<Int256>(0, 76, 2).value, value);
    }
}
