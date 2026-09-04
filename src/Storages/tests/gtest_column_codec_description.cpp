#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/dataTypeToAST.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTTupleDataType.h>
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

ASTPtr parseAlterColumnDeclaration(const String & declaration)
{
    ParserColumnDeclaration parser(
        /* require_type_ = */ true,
        /* allow_null_modifiers_ = */ false,
        /* check_keywords_after_name_ = */ false,
        /* allow_tuple_element_codec_removals_ = */ true);
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
    const auto & tuple_ast = declaration.getType()->as<ASTTupleDataType &>();
    const auto tuple_arguments = tuple_ast.getArguments();
    ASSERT_TRUE(tuple_arguments);
    const auto & id_type_ast = tuple_arguments->children[0]->as<ASTDataType &>();
    ASSERT_TRUE(id_type_ast.hasCodec());
    ASSERT_EQ(id_type_ast.children.size(), 1);
    EXPECT_EQ(id_type_ast.children.back().get(), id_type_ast.getCodec().get());

    const auto cloned = parsed->clone();
    EXPECT_EQ(cloned->getTreeHash(false), parsed->getTreeHash(false));
    const auto & cloned_declaration = cloned->as<ASTColumnDeclaration &>();
    const auto cloned_tuple_arguments = cloned_declaration.getType()->as<ASTTupleDataType &>().getArguments();
    ASSERT_TRUE(cloned_tuple_arguments);
    const auto & cloned_id_type_ast = cloned_tuple_arguments->children[0]->as<ASTDataType &>();
    ASSERT_TRUE(cloned_id_type_ast.hasCodec());
    EXPECT_NE(cloned_id_type_ast.getCodec().get(), id_type_ast.getCodec().get());

    DataTypePtr logical_type = DataTypeFactory::instance().get(declaration.getType());

    ColumnCodecDescription codec = codecDescriptionFromAST(
        declaration, logical_type, CodecValidationSettings::trusted());

    ASSERT_TRUE(codec.hasRoot());
    EXPECT_EQ(codec.getRoot()->formatWithSecretsOneLine(), "CODEC(LZ4)");
    ASSERT_EQ(codec.getCodecs().size(), 3);
    EXPECT_EQ(codec.getCodecs().at(CodecPath{})->formatWithSecretsOneLine(), "CODEC(LZ4)");
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"id"})->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"nested", "value"})->formatWithSecretsOneLine(), "CODEC(LZ4HC(4))");

    const auto element_codec = codec.resolve(CodecPath{"id"}, nullptr);
    ASSERT_TRUE(element_codec.codec);
    EXPECT_EQ(element_codec.codec->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(element_codec.declaration_path, CodecPath{"id"});
    EXPECT_FALSE(element_codec.codec_is_part_default);

    const auto inherited_root_codec = codec.resolve(CodecPath{"nested", "flag"}, nullptr);
    ASSERT_TRUE(inherited_root_codec.codec);
    EXPECT_EQ(inherited_root_codec.codec->formatWithSecretsOneLine(), "CODEC(LZ4)");
    EXPECT_TRUE(inherited_root_codec.declaration_path.empty());
    EXPECT_FALSE(inherited_root_codec.codec_is_part_default);

    auto without_root = codec.clone();
    without_root.resetRoot();
    EXPECT_FALSE(without_root.hasRoot());
    EXPECT_TRUE(without_root.hasSubcolumns());
    EXPECT_EQ(without_root.getCodecs().size(), 2);

    ASTColumnDeclaration restored;
    restored.name = declaration.name;
    restored.setType(dataTypeToAST(logical_type));
    applyCodecDescriptionToAST(restored, logical_type, codec);

    EXPECT_EQ(
        codecDescriptionFromAST(restored, logical_type, CodecValidationSettings::trusted()),
        codec);
    EXPECT_EQ(restored.formatWithSecretsOneLine(), declaration.formatWithSecretsOneLine());
}

TEST(ColumnCodecDescription, CodecOperationBelongsToElementDataType)
{
    const String declaration_text =
        "payload Tuple(items Array(Tuple(id UInt64 CODEC(ZSTD(3)), text String)), state Enum8('ok' = 1) CODEC(LZ4))";
    const auto parsed = parseColumnDeclaration(declaration_text);
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();

    const auto outer_arguments = declaration.getType()->as<ASTTupleDataType &>().getArguments();
    ASSERT_TRUE(outer_arguments);
    const auto array_arguments = outer_arguments->children[0]->as<ASTDataType &>().getArguments();
    ASSERT_TRUE(array_arguments);
    const auto inner_arguments = array_arguments->children[0]->as<ASTTupleDataType &>().getArguments();
    ASSERT_TRUE(inner_arguments);
    EXPECT_TRUE(inner_arguments->children[0]->as<ASTDataType &>().hasCodec());
    EXPECT_TRUE(outer_arguments->children[1]->as<ASTDataType &>().hasCodec());

    EXPECT_EQ(declaration.formatWithSecretsOneLine(), declaration_text);
    EXPECT_EQ(
        DataTypeFactory::instance().get(declaration.getType())->getName(),
        "Tuple(items Array(Tuple(id UInt64, text String)), state Enum8('ok' = 1))");
    EXPECT_EQ(parsed->clone()->getTreeHash(false), parsed->getTreeHash(false));

    const auto logical_type = DataTypeFactory::instance().get(declaration.getType());
    const auto codec = codecDescriptionFromAST(declaration, logical_type, CodecValidationSettings::trusted());
    ASSERT_EQ(codec.getCodecs().size(), 2);
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"items", "id"})->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"state"})->formatWithSecretsOneLine(), "CODEC(LZ4)");

    ASTColumnDeclaration restored;
    restored.name = declaration.name;
    restored.setType(dataTypeToAST(logical_type));
    applyCodecDescriptionToAST(restored, logical_type, codec);
    EXPECT_EQ(restored.formatWithSecretsOneLine(), declaration_text);

    const auto removal = parseAlterColumnDeclaration("payload Tuple(id UInt64 REMOVE CODEC, text String)");
    const auto removal_arguments = removal->as<ASTColumnDeclaration &>().getType()->as<ASTTupleDataType &>().getArguments();
    ASSERT_TRUE(removal_arguments);
    const auto & removed_type = removal_arguments->children[0]->as<ASTDataType &>();
    EXPECT_FALSE(removed_type.hasCodec());
    EXPECT_TRUE(removed_type.hasCodecRemoval());
    EXPECT_EQ(removal->formatWithSecretsOneLine(), "payload Tuple(id UInt64 REMOVE CODEC, text String)");
    EXPECT_EQ(removal->clone()->getTreeHash(false), removal->getTreeHash(false));
}

}
}
