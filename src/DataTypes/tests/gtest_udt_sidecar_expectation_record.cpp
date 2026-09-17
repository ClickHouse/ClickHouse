#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <gtest/gtest.h>

#include <array>
#include <string_view>

namespace DB::UDT
{
namespace
{

UUID uuid(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Digest digest(UInt8 first)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(first + index);
    return result;
}

String toHex(const Digest & value)
{
    constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(value.size() * 2);
    for (const UInt8 byte : value)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

SidecarExpectationRecord representativeRecord()
{
    return {
        .object = {
            .kind = SchemaObjectKind::Table,
            .database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL),
            .object_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe0fULL),
        },
        .object_schema_revision = 9,
        .sidecar_hash = digest(0x20),
        .physical_schema_fingerprint = digest(0x60),
    };
}

template <typename Callback>
void expectError(SidecarExpectationRecordError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected SidecarExpectationRecordError";
    }
    catch (const SidecarExpectationRecordError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(UDTSidecarExpectationRecord, CodecAndHashAreStable)
{
    const auto record = representativeRecord();
    const String encoded = encodeSidecarExpectationRecord(record);
    EXPECT_EQ(encoded.size(), sidecar_expectation_record_encoded_bytes);
    EXPECT_EQ(toHex(computeSidecarExpectationRecordHash(record)), "3caaea1fe9ce24c31d68609f9d0954d73d3a9155248f7286c077763341ba33db");
    EXPECT_EQ(decodeSidecarExpectationRecord(encoded), record);
}

TEST(UDTSidecarExpectationRecord, ExtendedCodecAndHashAreStable)
{
    const auto base_record = representativeRecord();
    const String base_encoded = encodeSidecarExpectationRecord(base_record);

    auto extended_record = base_record;
    extended_record.installation_record_hash = digest(0xa0);
    const String extended_encoded = encodeSidecarExpectationRecord(extended_record);
    EXPECT_EQ(extended_encoded.size(), sidecar_expectation_record_extended_encoded_bytes);
    EXPECT_EQ(std::string_view(extended_encoded).substr(0, base_encoded.size()), base_encoded);
    EXPECT_EQ(
        toHex(computeSidecarExpectationRecordHash(extended_record)), "0171e3665c4bc27d92ae41797892acda2b4e0a2ddb9a099f9f18f51084ada5c3");
    EXPECT_EQ(decodeSidecarExpectationRecord(extended_encoded), extended_record);
}

TEST(UDTSidecarExpectationRecord, AcceptsEveryObjectKindAndZeroDigestBits)
{
    for (const auto kind :
         std::array{SchemaObjectKind::Table, SchemaObjectKind::View, SchemaObjectKind::Dictionary, SchemaObjectKind::SyntheticTestObject})
    {
        auto record = representativeRecord();
        record.object.kind = kind;
        record.sidecar_hash = {};
        record.physical_schema_fingerprint = {};
        EXPECT_EQ(decodeSidecarExpectationRecord(encodeSidecarExpectationRecord(record)), record);

        record.installation_record_hash = Digest{};
        EXPECT_EQ(decodeSidecarExpectationRecord(encodeSidecarExpectationRecord(record)), record);
    }
}

TEST(UDTSidecarExpectationRecord, RejectsMalformedAndUnsupportedRecords)
{
    const auto record = representativeRecord();
    const String encoded = encodeSidecarExpectationRecord(record);
    expectError(
        SidecarExpectationRecordError::Code::Truncated,
        [&] { static_cast<void>(decodeSidecarExpectationRecord(std::string_view(encoded).substr(0, encoded.size() - 1))); });

    String trailing = encoded;
    trailing.push_back('\0');
    expectError(SidecarExpectationRecordError::Code::Truncated, [&] { static_cast<void>(decodeSidecarExpectationRecord(trailing)); });

    String truncated_extension = encoded;
    truncated_extension.append(sizeof(Digest) - 1, '\0');
    expectError(
        SidecarExpectationRecordError::Code::Truncated, [&] { static_cast<void>(decodeSidecarExpectationRecord(truncated_extension)); });

    auto extended = record;
    extended.installation_record_hash = digest(0xa0);
    String extended_trailing = encodeSidecarExpectationRecord(extended);
    extended_trailing.push_back('\0');
    expectError(
        SidecarExpectationRecordError::Code::TrailingData, [&] { static_cast<void>(decodeSidecarExpectationRecord(extended_trailing)); });

    String unknown_version = encoded;
    unknown_version[0] = 2;
    expectError(
        SidecarExpectationRecordError::Code::UnsupportedVersion,
        [&] { static_cast<void>(decodeSidecarExpectationRecord(unknown_version)); });

    auto definition = record;
    definition.object.kind = SchemaObjectKind::TypeDefinition;
    expectError(
        SidecarExpectationRecordError::Code::InvalidValue, [&] { static_cast<void>(encodeSidecarExpectationRecord(definition)); });

    auto zero_revision = record;
    zero_revision.object_schema_revision = 0;
    expectError(
        SidecarExpectationRecordError::Code::InvalidValue, [&] { static_cast<void>(encodeSidecarExpectationRecord(zero_revision)); });

    auto extension = record;
    extension.semantic_extension_flags = 1;
    expectError(
        SidecarExpectationRecordError::Code::UnsupportedVersion, [&] { static_cast<void>(encodeSidecarExpectationRecord(extension)); });
}

}
}
