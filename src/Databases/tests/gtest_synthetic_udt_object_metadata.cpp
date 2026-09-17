#include <Databases/UDT/SyntheticObjectMetadata.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

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

PersistedTypeOccurrencePath path(UInt64 occurrence_ordinal, std::vector<UInt64> children)
{
    return {
        .section = PersistedTypePathSection::SyntheticPayload,
        .object_ordinal = 0,
        .occurrence_ordinal = occurrence_ordinal,
        .type_child_ordinals = std::move(children),
    };
}

SyntheticObjectPhysicalOccurrence
occurrence(PersistedTypeOccurrencePath occurrence_path, const String & physical_name, SemanticCapabilityMask capabilities)
{
    const auto type = DataTypeFactory::instance().get(physical_name);
    return {
        .path = std::move(occurrence_path),
        .canonical_physical_type = type->getName(),
        .storage_fingerprint = physicalTypeFingerprint(type),
        .selected_semantic_capabilities = capabilities,
    };
}

SyntheticObjectMetadata metadata()
{
    return makeSyntheticObjectMetadata(
        {
            .kind = SchemaObjectKind::SyntheticTestObject,
            .database_uuid = uuid(1, 2),
            .object_uuid = uuid(3, 4),
        },
        7,
        "synthetic.fixture",
        {
            occurrence(path(0, {}), "UInt64", semanticCapabilityBit(SemanticCapability::Input)),
            occurrence(path(1, {0}), "String", semanticCapabilityBit(SemanticCapability::Output)),
        });
}

template <typename Callback>
void expectMetadataError(SyntheticObjectMetadataError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected SyntheticObjectMetadataError";
    }
    catch (const SyntheticObjectMetadataError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(SyntheticObjectMetadata, CanonicalRoundTripStartupValidationAndBinding)
{
    const auto original = metadata();
    const String bytes = encodeSyntheticObjectMetadata(original);
    const auto decoded = decodeSyntheticObjectMetadata(bytes);
    EXPECT_EQ(decoded, original);
    EXPECT_EQ(encodeSyntheticObjectMetadata(decoded), bytes);
    EXPECT_EQ(computeSyntheticObjectPhysicalFingerprint(decoded), decoded.physical_schema_fingerprint);
    EXPECT_EQ(computeSyntheticObjectMetadataRecordHash(decoded), decoded.canonical_record_hash);

    const SidecarExpectationRecord expectation{
        .object = decoded.object,
        .object_schema_revision = decoded.object_schema_revision,
        .sidecar_hash = digest(0x40),
        .physical_schema_fingerprint = decoded.physical_schema_fingerprint,
    };
    const auto validated = validateSyntheticDependentObjectMetadata(expectation, bytes);
    EXPECT_EQ(validated.object, decoded.object);
    EXPECT_EQ(validated.object_schema_revision, decoded.object_schema_revision);
    EXPECT_EQ(validated.physical_schema_fingerprint, decoded.physical_schema_fingerprint);

    const auto physical = makeSyntheticBoundPhysicalSchema(decoded);
    EXPECT_EQ(physical.object, decoded.object);
    EXPECT_EQ(physical.object_schema_revision, decoded.object_schema_revision);
    EXPECT_EQ(physical.physical_schema_fingerprint, decoded.physical_schema_fingerprint);
    ASSERT_EQ(physical.occurrences.size(), 2);
    EXPECT_EQ(physical.occurrences[0].physical_type->getName(), "UInt64");
    EXPECT_EQ(physical.occurrences[1].physical_type->getName(), "String");
    EXPECT_EQ(physical.occurrences[0].path, decoded.occurrences[0].path);
    EXPECT_EQ(physical.occurrences[1].selected_semantic_capabilities, decoded.occurrences[1].selected_semantic_capabilities);

    const auto cloned = decoded;
    EXPECT_EQ(cloned, decoded);
    EXPECT_EQ(encodeSyntheticObjectMetadata(cloned), bytes);
}

TEST(SyntheticObjectMetadata, CorruptionAndNonCanonicalBytesFailClosed)
{
    const auto original = metadata();
    const String bytes = encodeSyntheticObjectMetadata(original);
    for (size_t size = 0; size < bytes.size(); ++size)
    {
        expectMetadataError(
            SyntheticObjectMetadataError::Code::Truncated,
            [&] { static_cast<void>(decodeSyntheticObjectMetadata(std::string_view(bytes).substr(0, size))); });
    }

    String trailing = bytes;
    trailing.push_back('x');
    expectMetadataError(
        SyntheticObjectMetadataError::Code::TrailingData,
        [&] { static_cast<void>(decodeSyntheticObjectMetadata(trailing)); });

    String version = bytes;
    version[0] = 2;
    expectMetadataError(
        SyntheticObjectMetadataError::Code::UnsupportedVersion,
        [&] { static_cast<void>(decodeSyntheticObjectMetadata(version)); });

    String kind = bytes;
    kind[2] = 1;
    expectMetadataError(
        SyntheticObjectMetadataError::Code::InvalidValue,
        [&] { static_cast<void>(decodeSyntheticObjectMetadata(kind)); });

    String nonminimal = bytes;
    constexpr size_t diagnostic_frame_offset = 2 + 1 + 16 + 16 + 8;
    nonminimal[diagnostic_frame_offset] = static_cast<char>(0x80 | static_cast<UInt8>(original.diagnostic_name.size()));
    nonminimal.insert(nonminimal.begin() + static_cast<ptrdiff_t>(diagnostic_frame_offset + 1), '\0');
    expectMetadataError(
        SyntheticObjectMetadataError::Code::NonCanonical,
        [&] { static_cast<void>(decodeSyntheticObjectMetadata(nonminimal)); });

    String physical_fingerprint = bytes;
    physical_fingerprint[physical_fingerprint.size() - 68] ^= 1;
    expectMetadataError(
        SyntheticObjectMetadataError::Code::FingerprintMismatch,
        [&] { static_cast<void>(decodeSyntheticObjectMetadata(physical_fingerprint)); });

    String record_hash = bytes;
    record_hash[diagnostic_frame_offset + 1] ^= 1;
    expectMetadataError(
        SyntheticObjectMetadataError::Code::FingerprintMismatch,
        [&] { static_cast<void>(decodeSyntheticObjectMetadata(record_hash)); });
}

TEST(SyntheticObjectMetadata, StructuralLimitsExpectationAndPhysicalTypeAreExact)
{
    auto original = metadata();
    std::swap(original.occurrences[0], original.occurrences[1]);
    expectMetadataError(
        SyntheticObjectMetadataError::Code::NonCanonical,
        [&] { static_cast<void>(encodeSyntheticObjectMetadata(original)); });

    original = metadata();
    SyntheticObjectMetadataLimits limits;
    limits.maximum_occurrences = 1;
    expectMetadataError(
        SyntheticObjectMetadataError::Code::LimitExceeded,
        [&] { static_cast<void>(encodeSyntheticObjectMetadata(original, limits)); });

    const String bytes = encodeSyntheticObjectMetadata(original);
    SidecarExpectationRecord expectation{
        .object = original.object,
        .object_schema_revision = original.object_schema_revision + 1,
        .sidecar_hash = digest(0x60),
        .physical_schema_fingerprint = original.physical_schema_fingerprint,
    };
    expectMetadataError(
        SyntheticObjectMetadataError::Code::FingerprintMismatch,
        [&] { static_cast<void>(validateSyntheticDependentObjectMetadata(expectation, bytes)); });

    auto mismatched_type = metadata();
    mismatched_type.occurrences[0].canonical_physical_type = "UInt8";
    mismatched_type.physical_schema_fingerprint = computeSyntheticObjectPhysicalFingerprint(mismatched_type);
    mismatched_type.canonical_record_hash = computeSyntheticObjectMetadataRecordHash(mismatched_type);
    static_cast<void>(encodeSyntheticObjectMetadata(mismatched_type));
    expectMetadataError(
        SyntheticObjectMetadataError::Code::PhysicalTypeMismatch,
        [&] { static_cast<void>(makeSyntheticBoundPhysicalSchema(mismatched_type)); });
}

TEST(SyntheticObjectMetadata, PhysicalFingerprintIgnoresIdentityRevisionAndLogicalRoles)
{
    const auto original = metadata();
    auto replacement = makeSyntheticObjectMetadata(
        {
            .kind = SchemaObjectKind::SyntheticTestObject,
            .database_uuid = uuid(10, 20),
            .object_uuid = uuid(30, 40),
        },
        original.object_schema_revision + 1,
        "renamed.synthetic.fixture",
        original.occurrences);
    EXPECT_EQ(replacement.physical_schema_fingerprint, original.physical_schema_fingerprint);
    EXPECT_NE(replacement.canonical_record_hash, original.canonical_record_hash);

    const Digest record_hash_before_role_change = replacement.canonical_record_hash;
    replacement.occurrences[0].selected_semantic_capabilities = semanticCapabilityBit(SemanticCapability::Output);
    replacement.physical_schema_fingerprint = computeSyntheticObjectPhysicalFingerprint(replacement);
    EXPECT_EQ(replacement.physical_schema_fingerprint, original.physical_schema_fingerprint);
    replacement.canonical_record_hash = computeSyntheticObjectMetadataRecordHash(replacement);
    EXPECT_NE(replacement.canonical_record_hash, record_hash_before_role_change);

    replacement.occurrences[0].storage_fingerprint[0] ^= 1;
    replacement.physical_schema_fingerprint = computeSyntheticObjectPhysicalFingerprint(replacement);
    EXPECT_NE(replacement.physical_schema_fingerprint, original.physical_schema_fingerprint);
}

}
}
