#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int METADATA_MISMATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

String makeMetadataJSON(const String & default_expression, const String & extra_json = "")
{
    /// The `columns` payload as `ColumnsDescription::toString` produces it. The versions that kept
    /// the redundant parentheses of the user (before `IAST::FormatSettings::ignore_redundant_parentheses`)
    /// stored `(y + 1)` where the current version stores `y + 1`, and the stored string is compared
    /// with the local one on every server restart.
    String columns = "columns format version: 1\n2 columns:\n`x` UInt64\tDEFAULT\t" + default_expression + "\n`y` UInt64\n";

    String escaped_columns;
    for (char c : columns)
    {
        if (c == '\n')
            escaped_columns += "\\n";
        else if (c == '\t')
            escaped_columns += "\\t";
        else if (c == '"' || c == '\\')
        {
            escaped_columns += '\\';
            escaped_columns += c;
        }
        else
            escaped_columns += c;
    }

    return R"({"format_name":"CSV","columns":")" + escaped_columns
        + R"(","mode":"unordered","after_processing":"keep")" + extra_json + "}";
}

void expectMetadataMismatch(const ObjectStorageQueueTableMetadata & left, const ObjectStorageQueueTableMetadata & right)
{
    try
    {
        left.checkEquals(right);
        FAIL() << "Expected METADATA_MISMATCH";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::METADATA_MISMATCH);
    }
}

}

TEST(ObjectStorageQueueTableMetadata, ColumnsComparisonIgnoresRedundantParentheses)
{
    auto plain = ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("y + 1"));
    auto parenthesized = ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("(y + 1)"));
    auto different = ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("y + 2"));

    /// A table created by a version that stored the redundant parentheses must be accepted
    /// by a version that does not store them, and vice versa.
    EXPECT_NO_THROW(plain.checkEquals(parenthesized));
    EXPECT_NO_THROW(parenthesized.checkEquals(plain));

    /// A genuinely different default expression is still rejected.
    expectMetadataMismatch(plain, different);
}

TEST(ObjectStorageQueueTableMetadata, ParallelInsertsOmittedFromLegacyJSON)
{
    auto missing = ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("y + 1"));
    auto with_true = ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("y + 1", R"(,"parallel_inserts":true)"));
    auto with_false = ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("y + 1", R"(,"parallel_inserts":false)"));

    EXPECT_EQ(missing.toString().find("parallel_inserts"), std::string::npos);
    EXPECT_NE(with_true.toString().find("parallel_inserts"), std::string::npos);
    EXPECT_NE(with_false.toString().find("parallel_inserts"), std::string::npos);

    EXPECT_FALSE(missing.parallel_inserts_is_known);
    EXPECT_FALSE(missing.parallel_inserts_present_in_keeper);
    EXPECT_TRUE(with_true.parallel_inserts_is_known);
    EXPECT_TRUE(with_true.parallel_inserts_present_in_keeper);
    EXPECT_TRUE(with_true.parallel_inserts.load());
    EXPECT_FALSE(with_false.parallel_inserts.load());

    EXPECT_NO_THROW(missing.checkEquals(with_true));
    EXPECT_NO_THROW(with_true.checkEquals(missing));
    EXPECT_NO_THROW(missing.checkEquals(with_false));
    EXPECT_NO_THROW(with_true.checkEquals(with_true));
    EXPECT_NO_THROW(with_false.checkEquals(with_false));
    expectMetadataMismatch(with_true, with_false);
    expectMetadataMismatch(with_false, with_true);

    ObjectStorageQueueTableMetadata copied(with_true);
    EXPECT_TRUE(copied.parallel_inserts_is_known);
    EXPECT_TRUE(copied.parallel_inserts_present_in_keeper);
    EXPECT_NE(copied.toString().find("parallel_inserts"), std::string::npos);
}

TEST(ObjectStorageQueueTableMetadata, ParallelInsertsRejectsNonBooleanJSON)
{
    for (const auto & extra : {R"(,"parallel_inserts":1)", R"(,"parallel_inserts":"true")", R"(,"parallel_inserts":{})"})
    {
        try
        {
            ObjectStorageQueueTableMetadata::parse(makeMetadataJSON("y + 1", extra));
            FAIL() << "Expected BAD_ARGUMENTS for " << extra;
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << extra;
        }
    }
}
