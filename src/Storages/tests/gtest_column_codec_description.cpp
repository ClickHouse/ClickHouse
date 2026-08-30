#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/dataTypeToAST.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnCodecDescription.h>

namespace DB
{
namespace
{

ASTPtr parseColumnDeclaration(const String & declaration)
{
    ParserColumnDeclaration parser;
    return parseQuery(
        parser,
        declaration,
        DBMS_DEFAULT_MAX_QUERY_SIZE,
        DBMS_DEFAULT_MAX_PARSER_DEPTH,
        DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
}

TEST(ColumnCodecDescription, ExtractAndApply)
{
    ASTPtr parsed = parseColumnDeclaration(
        "payload Tuple(id UInt64 CODEC(ZSTD(3)), nested Tuple(value String CODEC(LZ4HC(4)), flag UInt8)) CODEC(LZ4)");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    DataTypePtr logical_type = DataTypeFactory::instance().get(declaration.getType());

    ColumnCodecDescription codec = codecDescriptionFromAST(
        declaration, logical_type, CodecValidationSettings::trusted());

    ASSERT_TRUE(codec.hasRoot());
    EXPECT_EQ(codec.getRoot()->formatWithSecretsOneLine(), "CODEC(LZ4)");
    ASSERT_EQ(codec.getSubcolumns().size(), 2);
    EXPECT_EQ(codec.getSubcolumns().at(CodecPath{"id"})->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(codec.getSubcolumns().at(CodecPath{"nested", "value"})->formatWithSecretsOneLine(), "CODEC(LZ4HC(4))");

    ASTColumnDeclaration restored;
    restored.name = declaration.name;
    restored.setType(dataTypeToAST(logical_type));
    applyCodecDescriptionToAST(restored, codec);

    EXPECT_EQ(
        codecDescriptionFromAST(restored, logical_type, CodecValidationSettings::trusted()),
        codec);
    EXPECT_EQ(restored.formatWithSecretsOneLine(), declaration.formatWithSecretsOneLine());
}

}
}
