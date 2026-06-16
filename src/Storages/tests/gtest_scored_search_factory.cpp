#include <Common/Exception.h>
#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchFactory.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_FUNCTION;
}

namespace
{

using namespace DB;
using Family = ScoredSearchDescriptor::Family;

ASTPtr makeIdentifier(const String & name)
{
    return make_intrusive<ASTIdentifier>(name);
}

ASTPtr makeFunction(const String & name, std::vector<Field> args)
{
    auto function = make_intrusive<ASTFunction>();
    function->name = name;
    auto arguments = make_intrusive<ASTExpressionList>();
    for (auto & arg : args)
        arguments->children.push_back(make_intrusive<ASTLiteral>(std::move(arg)));
    function->arguments = arguments;
    function->children.push_back(function->arguments);
    return function;
}

}

TEST(ScoredSearchFactory, GetBm25Identifier)
{
    const auto & factory = ScoredSearchFactory::instance();
    auto descriptor = factory.get(makeIdentifier("bm25"));

    EXPECT_EQ(descriptor.name, "bm25");
    EXPECT_EQ(descriptor.family, Family::Text);
    ASSERT_EQ(descriptor.params.size(), 2u);
    EXPECT_EQ(descriptor.params[0].safeGet<Float64>(), 1.2);
    EXPECT_EQ(descriptor.params[1].safeGet<Float64>(), 0.75);
}

TEST(ScoredSearchFactory, GetBm25Function)
{
    const auto & factory = ScoredSearchFactory::instance();
    auto descriptor = factory.get(makeFunction("bm25", {Field(Float64(1.5)), Field(Float64(0.75))}));

    EXPECT_EQ(descriptor.name, "bm25");
    EXPECT_EQ(descriptor.family, Family::Text);
    ASSERT_EQ(descriptor.params.size(), 2u);
    EXPECT_EQ(descriptor.params[0].safeGet<Float64>(), 1.5);
    EXPECT_EQ(descriptor.params[1].safeGet<Float64>(), 0.75);
}

TEST(ScoredSearchFactory, GetBm25BadArity)
{
    const auto & factory = ScoredSearchFactory::instance();
    auto ast = makeFunction("bm25", {Field(Float64(1)), Field(Float64(2)), Field(Float64(3))});

    try
    {
        factory.get(ast);
        FAIL() << "expected BAD_ARGUMENTS";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(ScoredSearchFactory, GetUnknownScoring)
{
    const auto & factory = ScoredSearchFactory::instance();
    auto ast = makeIdentifier("tfidf");

    try
    {
        factory.get(ast);
        FAIL() << "expected UNKNOWN_FUNCTION";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_FUNCTION);
    }
}

TEST(ScoredSearchFactory, Bm25FamilyIsText)
{
    const auto & factory = ScoredSearchFactory::instance();
    auto descriptor = factory.get(makeIdentifier("bm25"));
    EXPECT_EQ(descriptor.family, Family::Text);
}
