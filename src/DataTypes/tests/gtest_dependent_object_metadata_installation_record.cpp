#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>

#include <gtest/gtest.h>

#include <array>
#include <span>
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

String toHex(std::span<const CanonicalByte> bytes)
{
    constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    String result;
    result.reserve(bytes.size() * 2);
    for (const UInt8 byte : bytes)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

DependentObjectMetadataInstallationRecord representativeRecord()
{
    return {
        .object = {
            .kind = SchemaObjectKind::Table,
            .database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL),
            .object_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe0fULL),
        },
        .object_schema_revision = 9,
        .object_name = "events_2026",
        .metadata_artifact_hash = digest(0x40),
    };
}

template <typename Callback>
void expectError(DependentObjectMetadataInstallationRecordError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected DependentObjectMetadataInstallationRecordError";
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(UDTDependentObjectMetadataInstallationRecord, CodecAndHashAreStable)
{
    const auto record = representativeRecord();
    const String encoded = encodeDependentObjectMetadataInstallationRecord(record);
    EXPECT_EQ(
        toHex(std::span<const CanonicalByte>(reinterpret_cast<const CanonicalByte *>(encoded.data()), encoded.size())),
        "01000100112233445566778899aabbccddeeff102132435465768798a9bacbdcedfe0f09000000000000000b6576656e74735f"
        "32303236404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f01000000");
    EXPECT_EQ(
        toHex(computeDependentObjectMetadataInstallationRecordHash(record)),
        "397971975c8b7da78fd5ed0bef5d710736ab2ccb974ecbb3ca31b652893f1b25");
    EXPECT_EQ(decodeDependentObjectMetadataInstallationRecord(encoded), record);
}

TEST(UDTDependentObjectMetadataInstallationRecord, AcceptsDependentObjectKindsAndExactNames)
{
    for (const auto kind :
         std::array{SchemaObjectKind::Table, SchemaObjectKind::View, SchemaObjectKind::Dictionary, SchemaObjectKind::SyntheticTestObject})
    {
        auto record = representativeRecord();
        record.object.kind = kind;
        record.object_name = "quoted/name.with spaces";
        record.metadata_artifact_hash = {};
        EXPECT_EQ(decodeDependentObjectMetadataInstallationRecord(encodeDependentObjectMetadataInstallationRecord(record)), record);
    }
}

TEST(UDTDependentObjectMetadataInstallationRecord, RejectsMalformedAndNonCanonicalRecords)
{
    const auto record = representativeRecord();
    const String encoded = encodeDependentObjectMetadataInstallationRecord(record);
    for (size_t size = 0; size < encoded.size(); ++size)
    {
        expectError(
            DependentObjectMetadataInstallationRecordError::Code::Truncated,
            [&] { static_cast<void>(decodeDependentObjectMetadataInstallationRecord(std::string_view(encoded).substr(0, size))); });
    }

    String trailing = encoded;
    trailing.push_back('\0');
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::TrailingData,
        [&] { static_cast<void>(decodeDependentObjectMetadataInstallationRecord(trailing)); });

    String nonminimal_name_size = encoded;
    nonminimal_name_size[43] = static_cast<char>(0x8b);
    nonminimal_name_size.insert(nonminimal_name_size.begin() + 44, '\0');
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::NonCanonical,
        [&] { static_cast<void>(decodeDependentObjectMetadataInstallationRecord(nonminimal_name_size)); });
}

TEST(UDTDependentObjectMetadataInstallationRecord, RejectsInvalidIdentityNameVersionAndBounds)
{
    auto record = representativeRecord();
    record.format_version = 2;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::UnsupportedVersion,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    record = representativeRecord();
    record.object.kind = SchemaObjectKind::TypeDefinition;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    record = representativeRecord();
    record.object.database_uuid = UUIDHelpers::Nil;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    record = representativeRecord();
    record.object_schema_revision = 0;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    record = representativeRecord();
    record.object_name.clear();
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    record = representativeRecord();
    record.object_name = String("bad\0name", 8);
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::InvalidValue,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    record = representativeRecord();
    record.semantic_extension_flags = 1;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::UnsupportedVersion,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(record)); });

    DependentObjectMetadataInstallationRecordLimits limits;
    limits.maximum_object_name_bytes = representativeRecord().object_name.size() - 1;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::LimitExceeded,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(representativeRecord(), limits)); });

    const String encoded = encodeDependentObjectMetadataInstallationRecord(representativeRecord());
    limits = {};
    limits.maximum_encoded_bytes = encoded.size() - 1;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::LimitExceeded,
        [&] { static_cast<void>(decodeDependentObjectMetadataInstallationRecord(encoded, limits)); });

    limits = {};
    limits.maximum_encoded_bytes = 0;
    expectError(
        DependentObjectMetadataInstallationRecordError::Code::InvalidConfiguration,
        [&] { static_cast<void>(encodeDependentObjectMetadataInstallationRecord(representativeRecord(), limits)); });
}

}
}
