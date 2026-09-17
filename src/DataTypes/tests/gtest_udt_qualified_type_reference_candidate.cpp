#include <DataTypes/UDT/QualifiedTypeReferenceCandidate.h>

#include <gtest/gtest.h>

namespace DB::UDT
{
namespace
{

TEST(UDTQualifiedTypeReferenceCandidate, RecognizesEveryParserQuotedIdentifierForm)
{
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("db.Type"));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("`db`.`Type`"));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("\"db\".\"Type\""));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("“db”.“Type”"));
}

TEST(UDTQualifiedTypeReferenceCandidate, RecognizesUnicodeTriviaAroundSeparator)
{
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("db\xC2\xA0.\xC2\xA0Type"));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("“db”\xE2\x80\x89.\xE2\x80\x89“Type”"));
}

TEST(UDTQualifiedTypeReferenceCandidate, SkipsStringLiteralsAndUnqualifiedIdentifiers)
{
    EXPECT_FALSE(hasQualifiedTypeReferenceCandidate("Tuple(name String, value UInt64)"));
    EXPECT_FALSE(hasQualifiedTypeReferenceCandidate("'db.Type'"));
    EXPECT_FALSE(hasQualifiedTypeReferenceCandidate("‘db.Type’"));
    EXPECT_FALSE(hasQualifiedTypeReferenceCandidate("“column” UInt64"));
    EXPECT_FALSE(hasQualifiedTypeReferenceCandidate("UInt64 /* db.Type is only a comment */"));
}

TEST(UDTQualifiedTypeReferenceCandidate, UnknownOrMalformedTokensAreConservative)
{
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("\xF0\x9F\x92\xA5"));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("“unterminated"));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("'unterminated"));
    EXPECT_TRUE(hasQualifiedTypeReferenceCandidate("UInt64 /* unterminated"));
}

}
}
