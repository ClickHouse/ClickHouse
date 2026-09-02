#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int METADATA_MISMATCH;
}

namespace
{

String makeMetadataJSON(const String & default_expression)
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

    return R"({"format_name":"CSV","columns":")" + escaped_columns + R"(","mode":"unordered","after_processing":"keep"})";
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
    try
    {
        plain.checkEquals(different);
        FAIL() << "Expected METADATA_MISMATCH";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::METADATA_MISMATCH);
    }
}
