#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/NamesAndTypes.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/dataTypeToAST.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTTupleElementCodecOperation.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnCodecDescription.h>
#include <Storages/ColumnCodecAST.h>
#include <Storages/ColumnCodecResolver.h>
#include <Storages/ColumnCodecValidation.h>
#include <Storages/ColumnsDescription.h>

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
        /* tuple_element_codec_syntax_ = */ TupleElementCodecSyntax::AllowSetAndRemove);
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
    const auto operations = tuple_ast.getCodecOperationsByElement();
    ASSERT_EQ(operations.size(), 2);
    const auto * id_operation = operations[0];
    ASSERT_TRUE(id_operation);
    ASSERT_EQ(id_operation->kind, TupleElementCodecOperationKind::Set);
    ASSERT_TRUE(id_operation->getCodec());

    const auto cloned = parsed->clone();
    EXPECT_EQ(cloned->getTreeHash(false), parsed->getTreeHash(false));
    const auto & cloned_declaration = cloned->as<ASTColumnDeclaration &>();
    const auto & cloned_tuple_ast = cloned_declaration.getType()->as<ASTTupleDataType &>();
    const auto cloned_operations = cloned_tuple_ast.getCodecOperationsByElement();
    ASSERT_EQ(cloned_operations.size(), 2);
    const auto * cloned_id_operation = cloned_operations[0];
    ASSERT_TRUE(cloned_id_operation);
    EXPECT_NE(cloned_id_operation->getCodec().get(), id_operation->getCodec().get());

    DataTypePtr logical_type = DataTypeFactory::instance().get(declaration.getType());

    ColumnCodecDescription codec = codecDescriptionFromAST(
        declaration, logical_type, CodecValidationSettings::trusted());

    ASSERT_TRUE(codec.hasRoot());
    EXPECT_EQ(codec.getRoot()->formatWithSecretsOneLine(), "CODEC(LZ4)");
    ASSERT_EQ(codec.getCodecs().size(), 3);
    EXPECT_EQ(codec.getCodecs().at(CodecPath{})->formatWithSecretsOneLine(), "CODEC(LZ4)");
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"id"})->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"nested", "value"})->formatWithSecretsOneLine(), "CODEC(LZ4HC(4))");

    const auto element_codec = codec.find(CodecPath{"id"});
    ASSERT_TRUE(element_codec.codec);
    EXPECT_EQ(element_codec.codec->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(element_codec.declaration_path, CodecPath{"id"});

    const auto inherited_root_codec = codec.find(CodecPath{"nested", "flag"});
    ASSERT_TRUE(inherited_root_codec.codec);
    EXPECT_EQ(inherited_root_codec.codec->formatWithSecretsOneLine(), "CODEC(LZ4)");
    EXPECT_TRUE(inherited_root_codec.declaration_path.empty());

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

TEST(ColumnCodecDescription, CodecOperationBelongsToOwningTuple)
{
    const String declaration_text =
        "c Tuple(items Array(Tuple(id UInt64 CODEC(ZSTD(3)), text String)), state Enum8('ok' = 1) CODEC(LZ4))";
    const String formatted_declaration_text =
        "`c` Tuple(items Array(Tuple(id UInt64 CODEC(ZSTD(3)), text String)), state Enum8('ok' = 1) CODEC(LZ4))";
    const auto parsed = parseColumnDeclaration(declaration_text);
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();

    const auto & outer_tuple = declaration.getType()->as<ASTTupleDataType &>();
    const auto outer_arguments = outer_tuple.getArguments();
    ASSERT_TRUE(outer_arguments);
    const auto array_arguments = outer_arguments->children[0]->as<ASTDataType &>().getArguments();
    ASSERT_TRUE(array_arguments);
    const auto & inner_tuple = array_arguments->children[0]->as<ASTTupleDataType &>();
    const auto inner_arguments = inner_tuple.getArguments();
    ASSERT_TRUE(inner_arguments);
    const auto inner_codec_operations = inner_tuple.getCodecOperationsByElement();
    const auto outer_codec_operations = outer_tuple.getCodecOperationsByElement();
    ASSERT_EQ(inner_codec_operations.size(), 2);
    ASSERT_EQ(outer_codec_operations.size(), 2);
    EXPECT_TRUE(inner_codec_operations[0]);
    EXPECT_TRUE(outer_codec_operations[1]);

    EXPECT_EQ(declaration.formatWithSecretsOneLine(), formatted_declaration_text);
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
    EXPECT_EQ(restored.formatWithSecretsOneLine(), formatted_declaration_text);

    const auto removal = parseAlterColumnDeclaration("c Tuple(id UInt64 REMOVE CODEC, text String)");
    const auto & removal_tuple = removal->as<ASTColumnDeclaration &>().getType()->as<ASTTupleDataType &>();
    const auto removal_operations = removal_tuple.getCodecOperationsByElement();
    ASSERT_EQ(removal_operations.size(), 2);
    const auto * removal_operation = removal_operations[0];
    ASSERT_TRUE(removal_operation);
    EXPECT_EQ(removal_operation->kind, TupleElementCodecOperationKind::Remove);
    EXPECT_FALSE(removal_operation->getCodec());
    EXPECT_EQ(removal->formatWithSecretsOneLine(), "`c` Tuple(id UInt64 REMOVE CODEC, text String)");
    EXPECT_EQ(removal->clone()->getTreeHash(false), removal->getTreeHash(false));
}

TEST(ColumnCodecDescription, NullableIsTransparentForTupleCodecPaths)
{
    const auto parsed = parseColumnDeclaration(
        "payload Array(Nullable(Tuple(id UInt64 CODEC(ZSTD(3)), text String))) CODEC(LZ4)");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto logical_type = DataTypeFactory::instance().get(declaration.getType());

    const auto codec = codecDescriptionFromAST(declaration, logical_type, CodecValidationSettings::trusted());
    ASSERT_EQ(codec.getCodecs().size(), 2);
    EXPECT_EQ(codec.getCodecs().at(CodecPath{})->formatWithSecretsOneLine(), "CODEC(LZ4)");
    EXPECT_EQ(codec.getCodecs().at(CodecPath{"id"})->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
    EXPECT_EQ(canonicalizeCodecPath(logical_type, CodecPath{"id"}), CodecPath{"id"});

    ASTColumnDeclaration restored;
    restored.name = declaration.name;
    restored.setType(dataTypeToAST(logical_type));
    applyCodecDescriptionToAST(restored, logical_type, codec);
    EXPECT_EQ(restored.formatWithSecretsOneLine(), declaration.formatWithSecretsOneLine());
}

TEST(ColumnCodecDescription, ImplicitOuterNullableIsValidatedAgainstResultingType)
{
    const auto parsed = parseColumnDeclaration(
        "payload Tuple(id UInt64 CODEC(ZSTD(3)), text String) CODEC(LZ4)");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto declared_type = DataTypeFactory::instance().get(declaration.getType());
    const auto resulting_type = DataTypeFactory::instance().get("Nullable(Tuple(id UInt64, text String))");

    const auto codec = codecDescriptionFromAST(
        declaration, declared_type, resulting_type, CodecValidationSettings::trusted());
    const ColumnCodecResolver resolver(
        codec,
        resulting_type,
        NameAndTypePair(declaration.name, resulting_type),
        nullptr);

    bool found_null_map = false;
    bool found_id_value = false;
    resulting_type->getDefaultSerialization()->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            if (path.empty())
                return;

            const auto stream = classifyCodecStream(path);
            const auto resolved = resolver.resolve(path);
            if (path.back().type == ISerialization::Substream::NullMap)
            {
                found_null_map = true;
                EXPECT_TRUE(stream.structural);
                EXPECT_TRUE(stream.logical_path.empty());
                ASSERT_TRUE(resolved.codec);
                EXPECT_EQ(resolved.codec->formatWithSecretsOneLine(), "CODEC(LZ4)");
            }
            else if (!stream.structural && stream.logical_path == CodecPath{"id"})
            {
                found_id_value = true;
                ASSERT_TRUE(resolved.codec);
                EXPECT_EQ(resolved.codec->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");
            }
        },
        resulting_type);

    EXPECT_TRUE(found_null_map);
    EXPECT_TRUE(found_id_value);
}

TEST(ColumnCodecDescription, OrdinaryTupleHasNoCodecOperations)
{
    const auto parsed = parseColumnDeclaration(
        "value Tuple(a UInt8, nested Tuple(e Enum8('x' = 1), s String))");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto & outer_tuple = declaration.getType()->as<ASTTupleDataType &>();
    EXPECT_FALSE(outer_tuple.getCodecOperations());

    const auto arguments = outer_tuple.getArguments();
    ASSERT_TRUE(arguments);
    const auto & inner_tuple = arguments->children[1]->as<ASTTupleDataType &>();
    EXPECT_FALSE(inner_tuple.getCodecOperations());
    EXPECT_EQ(DataTypeFactory::instance().get(declaration.getType())->getName(),
        "Tuple(a UInt8, nested Tuple(e Enum8('x' = 1), s String))");
    EXPECT_EQ(parsed->clone()->getTreeHash(false), parsed->getTreeHash(false));
}

TEST(ColumnCodecDescription, EphemeralDefaultUsesLogicalType)
{
    const auto parsed = parseColumnDeclaration(
        "value Tuple(a UInt8 CODEC(LZ4), b String) EPHEMERAL");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto & default_function = declaration.getDefaultExpression()->as<ASTFunction &>();
    ASSERT_TRUE(default_function.arguments);
    ASSERT_EQ(default_function.arguments->children.size(), 1);
    EXPECT_EQ(
        default_function.arguments->children.front()->as<ASTLiteral &>().value.safeGet<String>(),
        "Tuple(a UInt8, b String)");
}

TEST(ColumnCodecDescription, DormantDeclarationKeepsImplicitParameters)
{
    const auto parsed = parseColumnDeclaration(
        "value Tuple(a UInt64 CODEC(LZ4), b UInt64 CODEC(LZ4)) CODEC(Delta)");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto logical_type = DataTypeFactory::instance().get(declaration.getType());
    auto codec = codecDescriptionFromAST(declaration, logical_type, CodecValidationSettings::trusted());

    ASSERT_TRUE(codec.hasRoot());
    EXPECT_EQ(codec.getRoot()->formatWithSecretsOneLine(), "CODEC(Delta)");

    codec.erase(CodecPath{"a"});
    codec.erase(CodecPath{"b"});
    codec = validateColumnCodecDescription(codec, logical_type, CodecValidationSettings::trusted());
    EXPECT_EQ(codec.getRoot()->formatWithSecretsOneLine(), "CODEC(Delta(8))");
}

TEST(ColumnCodecDescription, StructuralIntrospectionPreservesSymbolicDefault)
{
    const auto parsed = parseColumnDeclaration(
        "payload Tuple(a Array(UInt64) CODEC(Delta, Default))");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto logical_type = DataTypeFactory::instance().get(declaration.getType());
    const auto codec = codecDescriptionFromAST(declaration, logical_type, CodecValidationSettings::trusted());
    const ColumnCodecResolver resolver(
        codec,
        logical_type,
        NameAndTypePair(declaration.name, logical_type),
        nullptr);

    bool found_array_offsets = false;
    IDataType::forEachSubcolumn(
        [&](const auto & path, const auto & name, const auto &)
        {
            if (name != "a.size0")
                return;

            found_array_offsets = true;
            const auto resolved = resolver.resolve(path);
            EXPECT_TRUE(resolved.stream.structural);
            ASSERT_TRUE(resolved.codec);
            EXPECT_EQ(resolved.codec->formatWithSecretsOneLine(), "CODEC(Default)");
        },
        ISerialization::SubstreamData(logical_type->getDefaultSerialization()).withType(logical_type));
    EXPECT_TRUE(found_array_offsets);
}

TEST(ColumnCodecDescription, VersionedColumnsMetadata)
{
    const auto parsed = parseColumnDeclaration(
        "payload Tuple(id UInt64 CODEC(ZSTD(3)), `literal.dot` String CODEC(LZ4)) CODEC(ZSTD(1))");
    const auto & declaration = parsed->as<ASTColumnDeclaration &>();
    const auto logical_type = DataTypeFactory::instance().get(declaration.getType());

    ColumnDescription column(declaration.name, logical_type);
    column.codec = codecDescriptionFromAST(declaration, logical_type, CodecValidationSettings::trusted());

    ColumnDescription root_only("root_only", logical_type);
    root_only.codec.setRoot(column.codec.getRoot());
    ColumnsDescription root_only_columns;
    root_only_columns.add(root_only);
    const String root_only_serialized = root_only_columns.toString(/* include_comments = */ true);
    EXPECT_TRUE(root_only_serialized.starts_with("columns format version: 1\n"));
    EXPECT_EQ(root_only_serialized.find("TUPLE_ELEMENT_CODECS"), String::npos);

    ColumnsDescription columns;
    columns.add(column);

    const String serialized = columns.toString(/* include_comments = */ true);
    EXPECT_TRUE(serialized.starts_with("columns format version: 2\n"));
    EXPECT_NE(serialized.find("TUPLE_ELEMENT_CODECS"), String::npos);
    EXPECT_EQ(serialized.find("Tuple(id UInt64 CODEC"), String::npos);

    const auto restored = ColumnsDescription::parse(serialized);
    EXPECT_EQ(restored.get("payload").codec, column.codec);
    EXPECT_EQ(restored.toString(/* include_comments = */ true), serialized);

    const String legacy =
        "columns format version: 1\n"
        "1 columns:\n"
        "`payload` Tuple(id UInt64 CODEC(ZSTD(3)), text String)\n";
    const auto migrated = ColumnsDescription::parse(legacy);
    EXPECT_EQ(
        migrated.get("payload").codec.getCodecs().at(CodecPath{"id"})->formatWithSecretsOneLine(),
        "CODEC(ZSTD(3))");
    EXPECT_TRUE(migrated.toString(/* include_comments = */ true).starts_with("columns format version: 2\n"));
}

}
}
