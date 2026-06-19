#include <gtest/gtest.h>

#include <config.h>
#if USE_PARQUET

#include <Common/Exception.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/Parquet/VariantBinaryDecoder.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <optional>
#include <string_view>
#include <vector>

namespace
{

using namespace DB;
using namespace DB::Parquet;
using namespace DB::Parquet::VariantReader;

char variantHeader(VariantBasicType basic_type, UInt8 value_header = 0)
{
    return static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) | static_cast<UInt8>(basic_type));
}

VariantMetadata makeMetadata()
{
    return VariantMetadata{{std::string_view{"a"}}, true};
}

String makeMetadataBlob(std::vector<std::string_view> strings, bool strings_sorted)
{
    String metadata;
    UInt8 header = 1;
    header |= static_cast<UInt8>(strings_sorted) << VARIANT_METADATA_SORTED_STRINGS_SHIFT;
    metadata.push_back(static_cast<char>(header));
    appendVariantLittleEndian(strings.size(), 1, metadata);

    UInt64 offset = 0;
    appendVariantLittleEndian(offset, 1, metadata);
    for (std::string_view string : strings)
    {
        offset += string.size();
        appendVariantLittleEndian(offset, 1, metadata);
    }

    for (std::string_view string : strings)
        metadata.append(string.data(), string.size());

    return metadata;
}

String makeEmptyObjectWithTrailingPayload()
{
    return String{variantHeader(VariantBasicType::Object), '\0', '\0', '\0'};
}

String makeObjectWithFirstFieldAndTrailingPayload()
{
    String value;
    value.push_back(variantHeader(VariantBasicType::Object));
    appendVariantLittleEndian(1, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(1, 1, value);
    value.push_back(variantHeader(VariantBasicType::Primitive, static_cast<UInt8>(VariantPrimitiveType::Null)));
    value.push_back('\0');
    return value;
}

String makeObjectWithFirstFieldAndInvalidSecondFieldId()
{
    String value;
    value.push_back(variantHeader(VariantBasicType::Object));
    appendVariantLittleEndian(2, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(1, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(1, 1, value);
    appendVariantLittleEndian(1, 1, value);
    value.push_back(variantHeader(VariantBasicType::Primitive, static_cast<UInt8>(VariantPrimitiveType::Null)));
    return value;
}

String makeObjectWithDuplicateFieldId()
{
    String value;
    value.push_back(variantHeader(VariantBasicType::Object));
    appendVariantLittleEndian(2, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(0, 1, value);
    appendVariantLittleEndian(1, 1, value);
    appendVariantLittleEndian(2, 1, value);
    value.push_back(variantHeader(VariantBasicType::Primitive, static_cast<UInt8>(VariantPrimitiveType::Null)));
    value.push_back(variantHeader(VariantBasicType::Primitive, static_cast<UInt8>(VariantPrimitiveType::Null)));
    return value;
}

String makeEmptyArrayWithTrailingPayload()
{
    return String{variantHeader(VariantBasicType::Array), '\0', '\0', '\0'};
}

String makeNullWithTrailingPayload()
{
    return String{variantHeader(VariantBasicType::Primitive, static_cast<UInt8>(VariantPrimitiveType::Null)), '\0'};
}

}

TEST(ParquetVariantBinaryDecoder, MetadataRejectsUnsortedDictionaryWithSortedFlag)
{
    const FormatSettings format_settings;

    EXPECT_THROW(decodeMetadata(makeMetadataBlob({"b", "a"}, true), format_settings), DB::Exception);
    EXPECT_NO_THROW(decodeMetadata(makeMetadataBlob({"a", "b"}, true), format_settings));
}

TEST(ParquetVariantBinaryDecoder, MetadataRejectsDuplicateDictionaryStrings)
{
    const FormatSettings format_settings;

    EXPECT_THROW(decodeMetadata(makeMetadataBlob({"a", "a"}, true), format_settings), DB::Exception);
    EXPECT_THROW(decodeMetadata(makeMetadataBlob({"a", "b", "a"}, false), format_settings), DB::Exception);
    EXPECT_NO_THROW(decodeMetadata(makeMetadataBlob({"a", "b"}, true), format_settings));
}

TEST(ParquetVariantBinaryDecoder, PathDecodeRejectsTrailingObjectPayload)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto parsed_path = parseVariantPath("a");
    const auto resolved_missing_path = resolveVariantPath(metadata, parseVariantPath("missing"));
    const auto value = makeEmptyObjectWithTrailingPayload();

    VariantValue value_result;
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, parsed_path, value_result, format_settings), DB::Exception);
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, resolved_missing_path, value_result, format_settings), DB::Exception);

    ScalarExactValue scalar_result;
    EXPECT_THROW(tryDecodeScalarExactValueByPath(metadata, value, resolved_missing_path, scalar_result, format_settings), DB::Exception);

    std::vector<std::optional<std::string_view>> slices;
    EXPECT_THROW(collectObjectFieldSlicesByResolvedIds(metadata, value, std::vector<UInt64>{0}, slices, format_settings), DB::Exception);
}

TEST(ParquetVariantBinaryDecoder, ObjectFieldSliceDecodeRejectsTrailingNonObjectPayload)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto value = makeNullWithTrailingPayload();

    std::vector<std::optional<std::string_view>> slices;
    EXPECT_THROW(collectObjectFieldSlicesByResolvedIds(metadata, value, std::vector<UInt64>{0}, slices, format_settings), DB::Exception);
}

TEST(ParquetVariantBinaryDecoder, ObjectDecodersRejectDuplicateFieldIds)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto value = makeObjectWithDuplicateFieldId();
    const auto parsed_path = parseVariantPath("a");
    const auto resolved_path = resolveVariantPath(metadata, parsed_path);

    EXPECT_THROW(decodeValue(metadata, value, format_settings), DB::Exception);

    VariantValue value_result;
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, parsed_path, value_result, format_settings), DB::Exception);
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, resolved_path, value_result, format_settings), DB::Exception);

    ScalarExactValue scalar_result;
    EXPECT_THROW(tryDecodeScalarExactValueByPath(metadata, value, resolved_path, scalar_result, format_settings), DB::Exception);

    std::vector<std::optional<std::string_view>> slices;
    EXPECT_THROW(collectObjectFieldSlicesByResolvedIds(metadata, value, std::vector<UInt64>{0}, slices, format_settings), DB::Exception);
}

TEST(ParquetVariantBinaryDecoder, ObjectFieldSliceDecodeRejectsTrailingPayloadAfterRequestedField)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto value = makeObjectWithFirstFieldAndTrailingPayload();

    std::vector<std::optional<std::string_view>> slices;
    EXPECT_THROW(collectObjectFieldSlicesByResolvedIds(metadata, value, std::vector<UInt64>{0}, slices, format_settings), DB::Exception);
}

TEST(ParquetVariantBinaryDecoder, ObjectFieldSliceDecodeValidatesFieldsAfterRequestedField)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto value = makeObjectWithFirstFieldAndInvalidSecondFieldId();

    std::vector<std::optional<std::string_view>> slices;
    EXPECT_THROW(collectObjectFieldSlicesByResolvedIds(metadata, value, std::vector<UInt64>{0}, slices, format_settings), DB::Exception);
}

TEST(ParquetVariantBinaryDecoder, PathDecodeRejectsTrailingArrayPayload)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto parsed_path = parseVariantPath("a");
    const auto resolved_path = resolveVariantPath(metadata, parsed_path);
    const auto value = makeEmptyArrayWithTrailingPayload();

    VariantValue value_result;
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, parsed_path, value_result, format_settings), DB::Exception);
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, resolved_path, value_result, format_settings), DB::Exception);
}

TEST(ParquetVariantBinaryDecoder, MissingScalarPathRejectsTrailingPayload)
{
    const FormatSettings format_settings;
    const auto metadata = makeMetadata();
    const auto parsed_path = parseVariantPath("a");
    const auto resolved_path = resolveVariantPath(metadata, parsed_path);
    const auto value = makeNullWithTrailingPayload();

    VariantValue value_result;
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, parsed_path, value_result, format_settings), DB::Exception);
    EXPECT_THROW(tryDecodeValueByPath(metadata, value, resolved_path, value_result, format_settings), DB::Exception);

    ScalarExactValue scalar_result;
    EXPECT_THROW(tryDecodeScalarExactValueByPath(metadata, value, resolved_path, scalar_result, format_settings), DB::Exception);
}

#endif
