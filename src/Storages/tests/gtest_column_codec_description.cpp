#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnCodecPolicy.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{
namespace
{

ASTPtr parseColumnDeclaration(const String & text)
{
    ParserColumnDeclaration parser;
    return parseQuery(
        parser,
        text,
        DBMS_DEFAULT_MAX_QUERY_SIZE,
        DBMS_DEFAULT_MAX_PARSER_DEPTH,
        DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
}

ASTPtr parseCodec(const String & text)
{
    const auto declaration = parseColumnDeclaration("value UInt64 " + text);
    return declaration->as<ASTColumnDeclaration &>().getCodec()->clone();
}

TEST(ColumnCodecPolicy, DeepestTupleElementDeclarationWins)
{
    const auto root = parseCodec("CODEC(LZ4)");
    TupleElementCodecs elements{
        {{"nested"}, parseCodec("CODEC(ZSTD(1))")},
        {{"nested", "value"}, parseCodec("CODEC(NONE)")},
    };
    const ColumnCodecPolicyView policy(root, &elements);

    const auto exact = policy.find({"nested", "value"});
    ASSERT_TRUE(exact);
    EXPECT_EQ(exact->declaration_path, (TupleCodecPath{"nested", "value"}));
    EXPECT_EQ(exact->codec->formatWithSecretsOneLine(), "CODEC(NONE)");

    const auto inherited = policy.find({"nested", "other"});
    ASSERT_TRUE(inherited);
    EXPECT_EQ(inherited->declaration_path, (TupleCodecPath{"nested"}));
    EXPECT_EQ(inherited->codec->formatWithSecretsOneLine(), "CODEC(ZSTD(1))");

    const auto root_fallback = policy.find({"other"});
    ASSERT_TRUE(root_fallback);
    EXPECT_TRUE(root_fallback->declaration_path.empty());
    EXPECT_EQ(root_fallback->codec->formatWithSecretsOneLine(), "CODEC(LZ4)");
}

TEST(ColumnCodecPolicy, ColumnsSerializationSeparatesPhysicalSchemaAndCodecPolicy)
{
    ColumnsDescription columns{
        ColumnDescription(
            "payload",
            DataTypeFactory::instance().get("Tuple(id UInt64, text String)"),
            parseCodec("CODEC(LZ4)"),
            {})};
    columns.setTupleElementCodec("payload", {"id"}, parseCodec("CODEC(ZSTD(3))"));

    const String physical = columns.toString(/* include_comments = */ false);
    EXPECT_EQ(physical.find("subcolumn codec declarations"), String::npos);

    const String complete = columns.toStringWithCodecDeclarations(/* include_comments = */ false);
    EXPECT_NE(complete.find("subcolumn codec declarations"), String::npos);
    EXPECT_NE(complete.find("CODEC(ZSTD(3))"), String::npos);

    const auto restored = ColumnsDescription::parse(complete);
    ASSERT_TRUE(restored.get("payload").codec);
    EXPECT_EQ(restored.get("payload").codec->formatWithSecretsOneLine(), "CODEC(LZ4)");
    const auto * restored_elements = restored.tryGetTupleElementCodecs("payload");
    ASSERT_TRUE(restored_elements);
    ASSERT_EQ(restored_elements->size(), 1);
    EXPECT_EQ(restored_elements->at({"id"})->formatWithSecretsOneLine(), "CODEC(ZSTD(3))");

    const auto physical_only = ColumnsDescription::parse(physical);
    EXPECT_FALSE(physical_only.hasTupleElementCodecs());
}

}
}
