#include <gtest/gtest.h>

#include <Functions/FunctionGenerateRandomStructure.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

using namespace DB;

/// The depth-limit fallback in writeRandomType (depth > MAX_DEPTH) is unreachable from SQL at the
/// shipped MAX_DEPTH=16: a 36M-seed sweep of generateRandomStructure maxed out at recursion depth 16,
/// one short of the 17 needed to enter the guard. This test drives that branch deterministically with
/// a reduced max_depth so it can prove the fallback emits exactly one valid type (regression for the
/// missing `return;`, which let control fall through and concatenate a second type without a separator,
/// e.g. "Decimal32(7)IPv4", surfacing as SYNTAX_ERROR).
TEST(GenerateRandomStructure, DepthLimitFallbackEmitsSingleValidType)
{
    /// max_depth = 0 forces every seed straight into the fallback (depth 1 > 0 on the first nested type),
    /// so we exercise the guard for a large space of seeds. Every result must parse as a single type.
    for (size_t seed = 0; seed < 20000; ++seed)
    {
        String type = FunctionGenerateRandomStructure::generateRandomTypeForTest(seed, /*allow_suspicious_lc_types=*/true, /*max_depth=*/0);

        ASSERT_FALSE(type.empty()) << "empty type for seed " << seed;

        /// A malformed fall-through produces two concatenated types with no separator; DataTypeFactory
        /// parses the whole string as ONE type and throws on the trailing garbage.
        DataTypePtr parsed;
        ASSERT_NO_THROW(parsed = DataTypeFactory::instance().get(type))
            << "unparseable type for seed " << seed << ": " << type;

        /// Round-trip: the reparsed type name must be self-consistent (guards against silently-accepted junk).
        ASSERT_NO_THROW(DataTypeFactory::instance().get(parsed->getName()))
            << "type name not reparseable for seed " << seed << ": " << type;
    }
}
