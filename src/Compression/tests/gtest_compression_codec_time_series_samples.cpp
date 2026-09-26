#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <gtest/gtest.h>

#include <optional>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

ASTPtr makeTimeSeriesSamplesCodec()
{
    auto selector = makeASTFunction("TimeSeriesSamples");
    selector->as<ASTFunction &>().setNoEmptyArgs(true);
    return makeASTFunction(
        "CODEC",
        std::move(selector),
        makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{3})));
}

const CompressionCodecMultiple * getMultiple(const CompressionCodecPtr & codec)
{
    return typeid_cast<const CompressionCodecMultiple *>(codec.get());
}

}

TEST(CompressionCodecTimeSeriesSamples, ResolvesTypedStreamsToExistingChains)
{
    auto & factory = CompressionCodecFactory::instance();
    const auto datetime_type = std::make_shared<DataTypeDateTime>();
    const auto timestamp_type = std::make_shared<DataTypeDateTime64>(3);
    const auto value32_type = std::make_shared<DataTypeFloat32>();
    const auto value_type = std::make_shared<DataTypeFloat64>();
    const auto codec_ast = makeTimeSeriesSamplesCodec();

    const auto datetime_codec = factory.get(codec_ast, datetime_type);
    const auto timestamp_codec = factory.get(codec_ast, timestamp_type);
    const auto value32_codec = factory.get(codec_ast, value32_type);
    const auto value_codec = factory.get(codec_ast, value_type);

    const auto * datetime_multiple = getMultiple(datetime_codec);
    ASSERT_NE(datetime_multiple, nullptr);
    const auto datetime_chain = datetime_multiple->getCodecs();
    ASSERT_EQ(datetime_chain.size(), 3);
    EXPECT_EQ(datetime_chain[0]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::Delta));
    EXPECT_EQ(datetime_chain[0]->getCodecDescription()->formatForErrorMessage(), "Delta(4)");
    EXPECT_EQ(datetime_chain[1]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::T64));
    EXPECT_EQ(datetime_chain[2]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::ZSTD));
    EXPECT_EQ(datetime_codec->getFullCodecDescription()->formatForErrorMessage(), "CODEC(Delta(4), T64, ZSTD(3))");

    const auto * timestamp_multiple = getMultiple(timestamp_codec);
    ASSERT_NE(timestamp_multiple, nullptr);
    const auto timestamp_chain = timestamp_multiple->getCodecs();
    ASSERT_EQ(timestamp_chain.size(), 3);
    EXPECT_EQ(timestamp_chain[0]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::Delta));
    EXPECT_EQ(timestamp_chain[0]->getCodecDescription()->formatForErrorMessage(), "Delta(8)");
    EXPECT_EQ(timestamp_chain[1]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::T64));
    EXPECT_EQ(timestamp_chain[2]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::ZSTD));
    EXPECT_EQ(timestamp_codec->getFullCodecDescription()->formatForErrorMessage(), "CODEC(Delta(8), T64, ZSTD(3))");

    const auto * value32_multiple = getMultiple(value32_codec);
    ASSERT_NE(value32_multiple, nullptr);
    const auto value32_chain = value32_multiple->getCodecs();
    ASSERT_EQ(value32_chain.size(), 2);
    EXPECT_EQ(value32_chain[0]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::Gorilla));
    EXPECT_EQ(value32_chain[0]->getCodecDescription()->formatForErrorMessage(), "Gorilla(4)");
    EXPECT_EQ(value32_chain[1]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::ZSTD));
    EXPECT_EQ(value32_codec->getFullCodecDescription()->formatForErrorMessage(), "CODEC(Gorilla(4), ZSTD(3))");

    const auto * value_multiple = getMultiple(value_codec);
    ASSERT_NE(value_multiple, nullptr);
    const auto value_chain = value_multiple->getCodecs();
    ASSERT_EQ(value_chain.size(), 2);
    EXPECT_EQ(value_chain[0]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::Gorilla));
    EXPECT_EQ(value_chain[0]->getCodecDescription()->formatForErrorMessage(), "Gorilla(8)");
    EXPECT_EQ(value_chain[1]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::ZSTD));
    EXPECT_EQ(value_codec->getFullCodecDescription()->formatForErrorMessage(), "CODEC(Gorilla(8), ZSTD(3))");

    EXPECT_NE(datetime_codec->getHash(), timestamp_codec->getHash());
    EXPECT_NE(timestamp_codec->getHash(), value_codec->getHash());
    EXPECT_NE(value32_codec->getHash(), value_codec->getHash());
    EXPECT_EQ(timestamp_codec->getHash(), factory.get(codec_ast, timestamp_type)->getHash());
}

TEST(CompressionCodecTimeSeriesSamples, StructuralStreamsKeepGenericOuterCodec)
{
    const auto codec = CompressionCodecFactory::instance().get(
        makeTimeSeriesSamplesCodec(), static_cast<const IDataType *>(nullptr), nullptr, /*only_generic=*/ true);

    EXPECT_EQ(codec->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::ZSTD));
    EXPECT_EQ(codec->getFullCodecDescription()->formatForErrorMessage(), "CODEC(ZSTD(3))");
}

TEST(CompressionCodecTimeSeriesSamples, NoTypeMarkerFailsClosedIfInvoked)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test triggers LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    const auto selector = CompressionCodecFactory::instance().get(
        makeASTFunction("CODEC", makeASTFunction("TimeSeriesSamples")), static_cast<const IDataType *>(nullptr));

    char source[16]{};
    char destination[32]{};
    EXPECT_THROW(selector->compress(source, sizeof(source), destination), Exception);

    char encoded[ICompressionCodec::getHeaderSize()]{};
    encoded[0] = static_cast<char>(CompressionMethodByte::NONE);
    encoded[1] = static_cast<char>(ICompressionCodec::getHeaderSize());
    encoded[5] = 1;
    EXPECT_THROW(selector->decompress(encoded, sizeof(encoded), destination), Exception);
#endif
}

TEST(CompressionCodecTimeSeriesSamples, UnsupportedTypesFailClosed)
{
    const auto codec_ast = makeTimeSeriesSamplesCodec();
    const auto unsupported_type = std::make_shared<DataTypeUInt64>();

    try
    {
        CompressionCodecFactory::instance().get(codec_ast, unsupported_type);
        FAIL() << "TimeSeriesSamples must reject unsupported direct value types";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(CompressionCodecTimeSeriesSamples, ValidationPreservesSelectorWhenSubstreamHashesDiffer)
{
    const auto column_type = std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeDateTime64>(3), std::make_shared<DataTypeFloat64>()});
    const auto codec_ast = makeTimeSeriesSamplesCodec();

    const auto preprocessed = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
        codec_ast, column_type, CodecValidationSettings::trusted());

    EXPECT_EQ(preprocessed, codec_ast);
    EXPECT_EQ(preprocessed->formatForErrorMessage(), "CODEC(TimeSeriesSamples, ZSTD(3))");
}

TEST(CompressionCodecTimeSeriesSamples, ArrayTupleSubstreamsUseTypedSelectorAndSkipArraySizes)
{
    const auto column_type = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeDateTime64>(3), std::make_shared<DataTypeFloat64>()}));
    const auto codec_ast = makeTimeSeriesSamplesCodec();
    auto & factory = CompressionCodecFactory::instance();

    bool saw_array_sizes = false;
    size_t typed_streams = 0;
    column_type->getDefaultSerialization()->enumerateStreams(
        [&](const ISerialization::SubstreamPath & path)
        {
            if (path.empty())
            {
                ADD_FAILURE() << "Array(Tuple(...)) must enumerate named substreams";
                return;
            }

            if (path.back().type == ISerialization::Substream::ArraySizes)
            {
                saw_array_sizes = true;
                const auto structural_codec = factory.get(codec_ast, nullptr, nullptr, /*only_generic=*/ true);
                EXPECT_EQ(structural_codec->getFullCodecDescription()->formatForErrorMessage(), "CODEC(ZSTD(3))");
                return;
            }

            if (!ISerialization::isSpecialCompressionAllowed(path))
                return;

            ++typed_streams;
            const auto & stream_type = path.back().data.type;
            if (!stream_type)
            {
                ADD_FAILURE() << "Typed Array(Tuple(...)) data stream has no type";
                return;
            }

            const auto stream_codec = factory.get(codec_ast, stream_type.get());
            const auto * multiple = getMultiple(stream_codec);
            ASSERT_NE(multiple, nullptr);
            const auto chain = multiple->getCodecs();
            const WhichDataType which(stream_type);
            if (which.isDateTime64())
            {
                ASSERT_EQ(chain.size(), 3);
                EXPECT_EQ(chain[0]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::Delta));
                EXPECT_EQ(chain[1]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::T64));
            }
            else if (which.isFloat64())
            {
                ASSERT_EQ(chain.size(), 2);
                EXPECT_EQ(chain[0]->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::Gorilla));
            }
            else
                ADD_FAILURE() << "Unexpected typed stream: " << stream_type->getName();

            EXPECT_EQ(chain.back()->getMethodByte(), static_cast<uint8_t>(CompressionMethodByte::ZSTD));
        },
        column_type);

    EXPECT_TRUE(saw_array_sizes);
    EXPECT_EQ(typed_streams, 2u);

    const auto preprocessed = factory.validateCodecAndGetPreprocessedAST(
        codec_ast, column_type, CodecValidationSettings::trusted());
    EXPECT_EQ(preprocessed, codec_ast);
    EXPECT_EQ(preprocessed->formatForErrorMessage(), "CODEC(TimeSeriesSamples, ZSTD(3))");
}

TEST(CompressionCodecTimeSeriesSamples, NoTypeDescriptionIsSafeAndExplicit)
{
    MutableColumns columns;
    columns.emplace_back(std::make_shared<DataTypeString>()->createColumn());
    for (size_t i = 0; i < 5; ++i)
        columns.emplace_back(std::make_shared<DataTypeUInt8>()->createColumn());
    columns.emplace_back(getSettingsTierEnum()->createColumn());
    columns.emplace_back(std::make_shared<DataTypeString>()->createColumn());

    ASSERT_NO_THROW(CompressionCodecFactory::instance().fillCodecDescriptions(columns));

    std::optional<size_t> selector_row;
    for (size_t i = 0; i < columns[0]->size(); ++i)
    {
        if (columns[0]->getDataAt(i) == "TimeSeriesSamples")
        {
            selector_row = i;
            break;
        }
    }

    ASSERT_TRUE(selector_row.has_value());
    EXPECT_EQ(columns[1]->getUInt(*selector_row), static_cast<uint8_t>(CompressionMethodByte::NONE));
    EXPECT_EQ(columns[2]->getUInt(*selector_row), 0u);
    EXPECT_EQ(columns[3]->getUInt(*selector_row), 0u);
    EXPECT_NE(columns[7]->getDataAt(*selector_row).find("Delta"), std::string_view::npos);
    EXPECT_NE(columns[7]->getDataAt(*selector_row).find("T64"), std::string_view::npos);
    EXPECT_NE(columns[7]->getDataAt(*selector_row).find("Gorilla"), std::string_view::npos);
    EXPECT_NE(columns[7]->getDataAt(*selector_row).find("no standalone on-disk codec"), std::string_view::npos);
}
