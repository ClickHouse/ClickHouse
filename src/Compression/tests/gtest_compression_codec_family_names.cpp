#include <gtest/gtest.h>

#include <Compression/CompressionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/isCompressionCodec.h>

using namespace DB;

/// The parser tells a codec declaration from a column named `codec` by a static list of codec family names,
/// which cannot be derived from the factory. Every registered family must be in that list.
TEST(CompressionCodecFamilyNames, ParserKnowsEveryRegisteredFamily)
{
    for (const auto & family_name : CompressionCodecFactory::instance().getAllRegisteredNames())
    {
        auto codec = makeASTFunction("CODEC", make_intrusive<ASTIdentifier>(family_name));
        EXPECT_TRUE(isCompressionCodec(codec)) << family_name;
    }

    EXPECT_TRUE(isCompressionCodec(makeASTFunction("CODEC", make_intrusive<ASTIdentifier>("Default"))));
    EXPECT_FALSE(isCompressionCodec(makeASTFunction("CODEC", make_intrusive<ASTIdentifier>("String"))));
}
