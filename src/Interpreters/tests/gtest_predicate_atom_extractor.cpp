#include <gtest/gtest.h>

#include <Interpreters/PredicateAtomExtractor.h>

using namespace DB;


TEST(PredicateAtomExtractor, ClassifyEquals)
{
    EXPECT_EQ(classifyPredicateFunction("equals"), "Equality");
    EXPECT_EQ(classifyPredicateFunction("notEquals"), "Equality");
}

TEST(PredicateAtomExtractor, ClassifyRange)
{
    EXPECT_EQ(classifyPredicateFunction("less"), "Range");
    EXPECT_EQ(classifyPredicateFunction("greater"), "Range");
    EXPECT_EQ(classifyPredicateFunction("lessOrEquals"), "Range");
    EXPECT_EQ(classifyPredicateFunction("greaterOrEquals"), "Range");
}

TEST(PredicateAtomExtractor, ClassifyIn)
{
    EXPECT_EQ(classifyPredicateFunction("in"), "In");
    EXPECT_EQ(classifyPredicateFunction("globalIn"), "In");
    EXPECT_EQ(classifyPredicateFunction("notIn"), "In");
    EXPECT_EQ(classifyPredicateFunction("globalNotIn"), "In");
}

TEST(PredicateAtomExtractor, ClassifyLike)
{
    EXPECT_EQ(classifyPredicateFunction("like"), "LikeSubstring");
    EXPECT_EQ(classifyPredicateFunction("ilike"), "LikeSubstring");
    EXPECT_EQ(classifyPredicateFunction("notLike"), "LikeSubstring");
    EXPECT_EQ(classifyPredicateFunction("notILike"), "LikeSubstring");
}

TEST(PredicateAtomExtractor, ClassifyIsNull)
{
    EXPECT_EQ(classifyPredicateFunction("isNull"), "IsNull");
    EXPECT_EQ(classifyPredicateFunction("isNotNull"), "IsNull");
}

TEST(PredicateAtomExtractor, ClassifyOther)
{
    EXPECT_EQ(classifyPredicateFunction("has"), "Other");
    EXPECT_EQ(classifyPredicateFunction("startsWith"), "Other");
    EXPECT_EQ(classifyPredicateFunction("unknown_function"), "Other");
}

TEST(PredicateAtomExtractor, ExtractFromNullptr)
{
    auto atoms = extractPredicateAtoms(nullptr);
    EXPECT_TRUE(atoms.empty());
}
