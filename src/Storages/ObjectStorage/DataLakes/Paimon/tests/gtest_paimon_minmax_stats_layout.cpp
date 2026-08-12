#include <config.h>

#if USE_AVRO

#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Paimon/BinaryRow.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PartitionPruner.h>
#include <base/types.h>

using namespace Paimon;

namespace
{

/// Builds a Paimon `BinaryRow` blob holding `arity` non-null 8-byte fields.
/// Layout: 4-byte big-endian arity, then the null bitset, then one 8-byte slot per field.
String makeRow(Int32 arity)
{
    const Int32 bit_set_width = ((arity + 63 + 8) / 64) * 8;

    String bytes;
    /// arity is stored big-endian.
    bytes.push_back(static_cast<char>((arity >> 24) & 0xFF));
    bytes.push_back(static_cast<char>((arity >> 16) & 0xFF));
    bytes.push_back(static_cast<char>((arity >> 8) & 0xFF));
    bytes.push_back(static_cast<char>(arity & 0xFF));

    /// Null bitset: all zeros -> no field is null.
    bytes.append(bit_set_width, '\0');
    /// Value slots.
    bytes.append(static_cast<size_t>(arity) * 8, '\0');

    return bytes;
}

}

/// `BinaryRow` stores its arity in the row header, which is what tells a legacy (positional) statistics
/// row how many columns it actually covers.
TEST(PaimonMinMaxStatsLayout, BinaryRowExposesArity)
{
    EXPECT_EQ(BinaryRow(makeRow(1)).getArity(), 1);
    EXPECT_EQ(BinaryRow(makeRow(2)).getArity(), 2);
    EXPECT_EQ(BinaryRow(makeRow(3)).getArity(), 3);
    /// The bitset width grows past one word here, so this also checks that the arity itself is not
    /// confused with the header layout it drives.
    EXPECT_EQ(BinaryRow(makeRow(100)).getArity(), 100);
}

/// Legacy value statistics (written without `_VALUE_STATS_COLS`) can only be addressed positionally, and
/// that is sound only when they cover the whole table schema.
TEST(PaimonMinMaxStatsLayout, LegacyStatsArePositionalOnlyForTheFullSchema)
{
    /// Statistics for every field of a 3-column table: position i describes field i.
    EXPECT_TRUE(legacyValueStatsArePositional(/*stats_arity=*/3, /*null_counts_size=*/3, /*schema_field_count=*/3));

    /// A file written with a projected write schema, e.g. columns `[f0, f2]` of a table `[f0, f1, f2]`.
    /// Reading it positionally would answer a predicate on `f1` with `f2`'s bounds and could prune a file
    /// that still contains matching rows, so this layout must be rejected.
    EXPECT_FALSE(legacyValueStatsArePositional(2, 2, 3));

    /// More statistics columns than schema fields is just as unattributable.
    EXPECT_FALSE(legacyValueStatsArePositional(4, 4, 3));

    /// The `_NULL_COUNTS` array is parallel to the statistics row; disagreement means an unknown layout.
    EXPECT_FALSE(legacyValueStatsArePositional(3, 2, 3));
    EXPECT_FALSE(legacyValueStatsArePositional(2, 3, 3));

    /// A malformed (negative) arity must not be accepted for a zero-field schema through a signedness
    /// conversion.
    EXPECT_FALSE(legacyValueStatsArePositional(-1, 0, 0));
}

#endif
