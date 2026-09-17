#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <string_view>
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

String noArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

String oneBinaryStringArgument()
{
    constexpr std::array<char, 9> bytes{1, 0, 1, 11, 4, 'A', 0, static_cast<char>(0xff), 1};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor
makeDescriptor(UInt8 seed, bool zero_digests = false, UUID database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL))
{
    const DefinitionIdentity identity{
        .database_uuid = database_uuid,
        .type_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe00ULL + seed),
        .revision = static_cast<UInt64>(10 + seed),
    };
    const Digest definition_hash = zero_digests ? Digest{} : digest(static_cast<UInt8>(0x10 + seed));
    const String canonical_arguments = seed == 1 ? oneBinaryStringArgument() : noArguments();
    const String canonical_physical_type = seed == 1 ? "Tuple(UInt64, String)" : "Array(UInt32)";
    const Digest storage_fingerprint = zero_digests ? Digest{} : digest(static_cast<UInt8>(0x40 + seed));
    const UInt16 checker_abi = seed == 1 ? 2 : 1;
    const Digest policy_semantic_hash = zero_digests ? Digest{} : digest(static_cast<UInt8>(0x80 + seed));
    const SemanticCapabilityMask semantic_capabilities
        = seed == 1 ? semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks) : 0;
    const Digest instantiation_semantic_hash = computeInstantiationSemanticHash({
        .definition_identity = identity,
        .definition_hash = definition_hash,
        .canonical_arguments_encoding = canonical_arguments,
        .canonical_physical_type = canonical_physical_type,
        .storage_fingerprint = storage_fingerprint,
        .checker_abi = checker_abi,
        .checker_charge_abi = 1,
        .policy_abi = 1,
        .function_registry_abi = 1,
        .policy_semantic_hash = policy_semantic_hash,
        .semantic_capabilities = semantic_capabilities,
    });
    return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        identity,
        definition_hash,
        canonical_arguments,
        canonical_physical_type,
        instantiation_semantic_hash,
        storage_fingerprint,
        checker_abi,
        1,
        1,
        1,
        policy_semantic_hash,
        semantic_capabilities,
        seed == 1 ? "db.Alpha" : "db.Beta");
}

PersistedTypeReferences representativeReferences()
{
    PersistedTypeReferences result;
    result.object = {
        .kind = SchemaObjectKind::Table,
        .database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL),
        .object_uuid = uuid(0xfedcba9876543210ULL, 0x0123456789abcdefULL),
    };
    result.object_schema_revision = 9;
    result.physical_schema_fingerprint = digest(0xc0);
    result.descriptors = {makeDescriptor(1), makeDescriptor(2)};
    std::sort(result.descriptors.begin(), result.descriptors.end(), [](const auto & lhs, const auto & rhs) { return lhs.stableLess(rhs); });
    result.occurrence_paths = {
        {
            .section = PersistedTypePathSection::ColumnType,
            .object_ordinal = 0,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {},
        },
        {
            .section = PersistedTypePathSection::ColumnType,
            .object_ordinal = 0,
            .occurrence_ordinal = 1,
            .type_child_ordinals = {0, 3},
        },
        {
            .section = PersistedTypePathSection::ColumnType,
            .object_ordinal = 4,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {2},
        },
    };
    result.uses = {
        {.path_id = 0, .descriptor_id = 0},
        {.path_id = 1, .descriptor_id = 1},
        {.path_id = 2, .descriptor_id = 0},
    };
    return result;
}

template <typename Callback>
void expectReferencesError(PersistedTypeReferencesError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected PersistedTypeReferencesError";
    }
    catch (const PersistedTypeReferencesError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

template <typename Callback>
void expectDescriptorError(DescriptorError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected DescriptorError";
    }
    catch (const DescriptorError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

String encodeVarUInt(UInt64 value)
{
    String result;
    do
    {
        UInt8 byte = static_cast<UInt8>(value & 0x7f);
        value >>= 7;
        if (value)
            byte = static_cast<UInt8>(byte | 0x80);
        result.push_back(static_cast<char>(byte));
    } while (value);
    return result;
}

UInt64 readVarUInt(const String & bytes, size_t & position)
{
    UInt64 result = 0;
    UInt8 shift = 0;
    while (true)
    {
        EXPECT_LT(position, bytes.size());
        const UInt8 byte = static_cast<UInt8>(bytes[position++]);
        result |= static_cast<UInt64>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0)
            return result;
        shift = static_cast<UInt8>(shift + 7);
    }
}

void replaceVarUInt(String & bytes, size_t position, UInt64 replacement)
{
    size_t end = position;
    static_cast<void>(readVarUInt(bytes, end));
    bytes.replace(position, end - position, encodeVarUInt(replacement));
}

struct FrameLocation
{
    size_t frame_begin = 0;
    size_t payload_begin = 0;
    size_t payload_size = 0;

    size_t frameEnd() const { return payload_begin + payload_size; }
};

std::vector<FrameLocation> dictionaryFrames(const String & bytes, size_t count_offset)
{
    size_t position = count_offset;
    const UInt64 count = readVarUInt(bytes, position);
    std::vector<FrameLocation> result;
    result.reserve(static_cast<size_t>(count));
    for (UInt64 index = 0; index < count; ++index)
    {
        const size_t frame_begin = position;
        const UInt64 payload_size = readVarUInt(bytes, position);
        EXPECT_LE(payload_size, bytes.size() - position);
        result.push_back({.frame_begin = frame_begin, .payload_begin = position, .payload_size = static_cast<size_t>(payload_size)});
        position += static_cast<size_t>(payload_size);
    }
    return result;
}

size_t nextDictionaryOffset(const std::vector<FrameLocation> & frames)
{
    EXPECT_FALSE(frames.empty());
    return frames.back().frameEnd();
}

constexpr size_t descriptor_count_offset = 2 + 1 + 16 + 16 + 8 + 32 + 2;

TEST(UDTPersistedTypeReferences, CodecAndSidecarHashAreStable)
{
    const auto references = representativeReferences();
    const String encoded = encodePersistedTypeReferences(references);
    EXPECT_EQ(persisted_type_references_sidecar_hash_domain, "ClickHouse UDT sidecar expectation V1");
    EXPECT_EQ(encoded.size(), 540);
    EXPECT_EQ(
        toHex(computePersistedTypeReferencesSidecarHash(references)), "0f32b3fbdb838db0c60cedba448aaa9a0a4f8f09cd0d44ea90aad89bc7cd55bc");
    EXPECT_EQ(decodePersistedTypeReferences(encoded), references);

    ASSERT_EQ(references.descriptors.front().getCanonicalArgumentsEncoding(), oneBinaryStringArgument());
    EXPECT_EQ(
        decodePersistedTypeReferences(encoded).descriptors.front().getCanonicalArgumentsEncoding(),
        references.descriptors.front().getCanonicalArgumentsEncoding());
    EXPECT_EQ(decodePersistedTypeReferences(encoded).occurrence_paths[1].occurrence_ordinal, 1);
}

TEST(UDTPersistedTypeReferences, TextCodecHasCanonicalGrammarAndParseFormatIdentity)
{
    const auto references = representativeReferences();
    const String text = formatPersistedTypeReferencesText(references);
    const String expected_object_line = "TYPE REFERENCES V1 (\n  OBJECT TABLE DATABASE UUID '00112233-4455-6677-8899-aabbccddeeff' "
                                        "OBJECT UUID 'fedcba98-7654-3210-0123-456789abcdef' REVISION 9 PHYSICAL FINGERPRINT '"
        + toHex(references.physical_schema_fingerprint) + "' PATH DICTIONARY V1 SEMANTIC EXTENSION V1 FLAGS 0";
    EXPECT_TRUE(text.starts_with(expected_object_line));
    EXPECT_NE(text.find("ARGUMENTS HEX '0100010b044100ff01'"), String::npos);
    EXPECT_NE(text.find("  USE PATH (1, 0, 0) REF 0"), String::npos);
    EXPECT_NE(text.find("  USE PATH (1, 0, 1, 0, 3) REF 1"), String::npos);
    EXPECT_TRUE(text.ends_with("  USE PATH (1, 4, 0, 2) REF 0\n)"));

    const auto parsed = parsePersistedTypeReferencesText(text);
    EXPECT_EQ(parsed, references);
    EXPECT_EQ(formatPersistedTypeReferencesText(parsed), text);
    EXPECT_EQ(encodePersistedTypeReferences(parsed), encodePersistedTypeReferences(references));
    EXPECT_EQ(parsed.descriptors.front().getCanonicalArgumentsEncoding(), oneBinaryStringArgument());

    auto escaped_name = references;
    const auto descriptor = escaped_name.descriptors.front();
    escaped_name.descriptors.front() = PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        descriptor.getDefinitionIdentity(),
        descriptor.getDefinitionHash(),
        descriptor.getCanonicalArgumentsEncoding(),
        descriptor.getCanonicalPhysicalType(),
        descriptor.getInstantiationSemanticHash(),
        descriptor.getStorageFingerprint(),
        descriptor.getCheckerABI(),
        descriptor.getCheckerChargeABI(),
        descriptor.getPolicyABI(),
        descriptor.getFunctionRegistryABI(),
        descriptor.getPolicySemanticHash(),
        descriptor.getSemanticCapabilities(),
        "db.Quo'te\\Alias\n");
    const String escaped_text = formatPersistedTypeReferencesText(escaped_name);
    EXPECT_NE(escaped_text.find("NAME 'db.Quo\\'te\\\\Alias\\n'"), String::npos);
    EXPECT_EQ(parsePersistedTypeReferencesText(escaped_text), escaped_name);
}

TEST(UDTPersistedTypeReferences, TextCodecRejectsVersionsTrailingDuplicatesAndNoncanonicalForms)
{
    const String canonical = formatPersistedTypeReferencesText(representativeReferences());

    String unknown_version = canonical;
    unknown_version.replace(0, String("TYPE REFERENCES V1").size(), "TYPE REFERENCES V2");
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion,
        [&] { static_cast<void>(parsePersistedTypeReferencesText(unknown_version)); });

    String unknown_path_version = canonical;
    const size_t path_version = unknown_path_version.find("PATH DICTIONARY V1");
    ASSERT_NE(path_version, String::npos);
    unknown_path_version[path_version + String("PATH DICTIONARY V").size()] = '2';
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion,
        [&] { static_cast<void>(parsePersistedTypeReferencesText(unknown_path_version)); });

    String trailing = canonical;
    trailing.push_back(' ');
    expectReferencesError(
        PersistedTypeReferencesError::Code::TrailingData, [&] { static_cast<void>(parsePersistedTypeReferencesText(trailing)); });
    expectReferencesError(
        PersistedTypeReferencesError::Code::Truncated,
        [&] { static_cast<void>(parsePersistedTypeReferencesText(std::string_view(canonical).substr(0, canonical.size() - 1))); });

    String duplicate_descriptor_id = canonical;
    const size_t second_descriptor = duplicate_descriptor_id.find("\n  REF 1 ");
    ASSERT_NE(second_descriptor, String::npos);
    duplicate_descriptor_id[second_descriptor + String("\n  REF ").size()] = '0';
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(parsePersistedTypeReferencesText(duplicate_descriptor_id)); });

    String duplicate_path = canonical;
    const String second_path = "  USE PATH (1, 0, 1, 0, 3) REF 1";
    const size_t second_path_position = duplicate_path.find(second_path);
    ASSERT_NE(second_path_position, String::npos);
    duplicate_path.replace(second_path_position, second_path.size(), "  USE PATH (1, 0, 0) REF 1");
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical, [&] { static_cast<void>(parsePersistedTypeReferencesText(duplicate_path)); });

    String noncanonical_revision = canonical;
    const size_t revision = noncanonical_revision.find(" REVISION 9 PHYSICAL FINGERPRINT ");
    ASSERT_NE(revision, String::npos);
    noncanonical_revision.insert(revision + String(" REVISION ").size(), "0");
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(parsePersistedTypeReferencesText(noncanonical_revision)); });
}

TEST(UDTPersistedTypeReferences, TextCodecEnforcesProspectiveTextCountFieldAndDepthLimits)
{
    const auto references = representativeReferences();
    const String text = formatPersistedTypeReferencesText(references);

    PersistedTypeReferencesLimits limits;
    limits.maximum_text_bytes = text.size() - 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded,
        [&] { static_cast<void>(formatPersistedTypeReferencesText(references, limits)); });
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(parsePersistedTypeReferencesText(text, limits)); });

    limits = {};
    limits.maximum_descriptors = 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(parsePersistedTypeReferencesText(text, limits)); });

    limits = {};
    limits.maximum_occurrence_paths = 2;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(parsePersistedTypeReferencesText(text, limits)); });

    limits = {};
    limits.maximum_canonical_arguments_bytes = oneBinaryStringArgument().size() - 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(parsePersistedTypeReferencesText(text, limits)); });

    limits = {};
    limits.maximum_path_depth = 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(parsePersistedTypeReferencesText(text, limits)); });
}

TEST(UDTPersistedTypeReferences, AcceptsAllFrozenObjectAndPathKindsAndZeroDigestBits)
{
    constexpr std::array object_kinds{
        SchemaObjectKind::Table,
        SchemaObjectKind::View,
        SchemaObjectKind::Dictionary,
        SchemaObjectKind::SyntheticTestObject,
    };
    constexpr std::array path_sections{
        PersistedTypePathSection::ColumnType,
        PersistedTypePathSection::ViewExpression,
        PersistedTypePathSection::DictionaryAttribute,
        PersistedTypePathSection::SyntheticPayload,
    };
    for (size_t index = 0; index < object_kinds.size(); ++index)
    {
        auto references = representativeReferences();
        references.object.kind = object_kinds[index];
        references.physical_schema_fingerprint = {};
        references.descriptors = {makeDescriptor(2, true)};
        references.occurrence_paths = {{
            .section = path_sections[index],
            .object_ordinal = 0,
            .occurrence_ordinal = 0,
            .type_child_ordinals = {},
        }};
        references.uses = {{.path_id = 0, .descriptor_id = 0}};
        PersistedTypeReferencesLimits root_path_limits;
        root_path_limits.maximum_path_depth = 0;
        EXPECT_EQ(
            decodePersistedTypeReferences(encodePersistedTypeReferences(references, root_path_limits), root_path_limits), references);
    }
}

TEST(UDTPersistedTypeReferences, RejectsObjectPathSectionMismatchAndCrossDatabaseDescriptors)
{
    constexpr std::array object_kinds{
        SchemaObjectKind::Table,
        SchemaObjectKind::View,
        SchemaObjectKind::Dictionary,
        SchemaObjectKind::SyntheticTestObject,
    };
    constexpr std::array path_sections{
        PersistedTypePathSection::ColumnType,
        PersistedTypePathSection::ViewExpression,
        PersistedTypePathSection::DictionaryAttribute,
        PersistedTypePathSection::SyntheticPayload,
    };
    for (size_t object_index = 0; object_index < object_kinds.size(); ++object_index)
    {
        for (size_t path_index = 0; path_index < path_sections.size(); ++path_index)
        {
            if (object_index == path_index)
                continue;
            auto references = representativeReferences();
            references.object.kind = object_kinds[object_index];
            references.occurrence_paths = {{
                .section = path_sections[path_index],
                .object_ordinal = 0,
                .occurrence_ordinal = 0,
                .type_child_ordinals = {},
            }};
            references.uses = {{.path_id = 0, .descriptor_id = 0}};
            references.descriptors = {makeDescriptor(2)};
            expectReferencesError(
                PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(encodePersistedTypeReferences(references)); });
        }
    }

    const String valid = encodePersistedTypeReferences(representativeReferences());
    const auto descriptor_frames = dictionaryFrames(valid, descriptor_count_offset);
    const size_t path_count_offset = nextDictionaryOffset(descriptor_frames);
    const auto path_frames = dictionaryFrames(valid, path_count_offset);
    ASSERT_FALSE(path_frames.empty());
    String mismatched_section = valid;
    mismatched_section[path_frames.front().payload_begin] = static_cast<char>(PersistedTypePathSection::ViewExpression);
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(decodePersistedTypeReferences(mismatched_section)); });

    auto cross_database = representativeReferences();
    cross_database.descriptors = {
        makeDescriptor(1, false, uuid(0x1111222233334444ULL, 0x5555666677778888ULL)),
        makeDescriptor(2, false, uuid(0x1111222233334444ULL, 0x5555666677778888ULL)),
    };
    std::sort(
        cross_database.descriptors.begin(),
        cross_database.descriptors.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.stableLess(rhs); });
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(encodePersistedTypeReferences(cross_database)); });

    cross_database.object.database_uuid = uuid(0x1111222233334444ULL, 0x5555666677778888ULL);
    String cross_database_bytes = encodePersistedTypeReferences(cross_database);
    constexpr size_t object_database_uuid_offset = sizeof(UInt16) + sizeof(UInt8);
    cross_database_bytes[object_database_uuid_offset] ^= 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue,
        [&] { static_cast<void>(decodePersistedTypeReferences(cross_database_bytes)); });
}

TEST(UDTPersistedTypeReferences, PersistenceFactoryEnforcesEveryAvailableInvariant)
{
    const auto valid = makeDescriptor(2);
    const auto & identity = valid.getDefinitionIdentity();
    const auto rebuild = [&](DefinitionIdentity replacement_identity,
                             String replacement_arguments,
                             String replacement_physical_type,
                             Digest replacement_instantiation_hash,
                             UInt16 replacement_checker_abi,
                             SemanticCapabilityMask replacement_capabilities,
                             String replacement_name)
    {
        return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
            replacement_identity,
            valid.getDefinitionHash(),
            std::move(replacement_arguments),
            std::move(replacement_physical_type),
            replacement_instantiation_hash,
            valid.getStorageFingerprint(),
            replacement_checker_abi,
            valid.getCheckerChargeABI(),
            valid.getPolicyABI(),
            valid.getFunctionRegistryABI(),
            valid.getPolicySemanticHash(),
            replacement_capabilities,
            std::move(replacement_name));
    };

    auto nil_identity = identity;
    nil_identity.type_uuid = UUIDHelpers::Nil;
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            static_cast<void>(rebuild(
                nil_identity,
                valid.getCanonicalArgumentsEncoding(),
                valid.getCanonicalPhysicalType(),
                valid.getInstantiationSemanticHash(),
                valid.getCheckerABI(),
                valid.getSemanticCapabilities(),
                valid.getLastKnownQualifiedName()));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidArguments,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                "not canonical",
                valid.getCanonicalPhysicalType(),
                valid.getInstantiationSemanticHash(),
                valid.getCheckerABI(),
                valid.getSemanticCapabilities(),
                valid.getLastKnownQualifiedName()));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidPhysicalType,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                valid.getCanonicalArgumentsEncoding(),
                {},
                valid.getInstantiationSemanticHash(),
                valid.getCheckerABI(),
                valid.getSemanticCapabilities(),
                valid.getLastKnownQualifiedName()));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidPhysicalType,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                valid.getCanonicalArgumentsEncoding(),
                String{"Array\0(UInt32)", 13},
                valid.getInstantiationSemanticHash(),
                valid.getCheckerABI(),
                valid.getSemanticCapabilities(),
                valid.getLastKnownQualifiedName()));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                valid.getCanonicalArgumentsEncoding(),
                valid.getCanonicalPhysicalType(),
                valid.getInstantiationSemanticHash(),
                valid.getCheckerABI(),
                valid.getSemanticCapabilities(),
                {}));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                valid.getCanonicalArgumentsEncoding(),
                valid.getCanonicalPhysicalType(),
                valid.getInstantiationSemanticHash(),
                3,
                valid.getSemanticCapabilities(),
                valid.getLastKnownQualifiedName()));
        });
    expectDescriptorError(
        DescriptorError::Code::InvalidDefinition,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                valid.getCanonicalArgumentsEncoding(),
                valid.getCanonicalPhysicalType(),
                valid.getInstantiationSemanticHash(),
                valid.getCheckerABI(),
                0x80,
                valid.getLastKnownQualifiedName()));
        });
    expectDescriptorError(
        DescriptorError::Code::ConflictingIdentity,
        [&]
        {
            static_cast<void>(rebuild(
                identity,
                valid.getCanonicalArgumentsEncoding(),
                valid.getCanonicalPhysicalType(),
                {},
                valid.getCheckerABI(),
                valid.getSemanticCapabilities(),
                valid.getLastKnownQualifiedName()));
        });
}

TEST(UDTPersistedTypeReferences, RejectsTruncationVersionsAndNonminimalCounts)
{
    const String encoded = encodePersistedTypeReferences(representativeReferences());
    for (size_t size = 0; size < encoded.size(); ++size)
    {
        expectReferencesError(
            PersistedTypeReferencesError::Code::Truncated,
            [&] { static_cast<void>(decodePersistedTypeReferences(std::string_view(encoded).substr(0, size))); });
    }

    String trailing = encoded;
    trailing.push_back(0);
    expectReferencesError(
        PersistedTypeReferencesError::Code::TrailingData, [&] { static_cast<void>(decodePersistedTypeReferences(trailing)); });

    String version = encoded;
    version[0] = 2;
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion, [&] { static_cast<void>(decodePersistedTypeReferences(version)); });

    String object_kind = encoded;
    object_kind[2] = static_cast<char>(SchemaObjectKind::TypeDefinition);
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(decodePersistedTypeReferences(object_kind)); });

    String zero_revision = encoded;
    constexpr size_t object_revision_offset = 2 + 1 + 16 + 16;
    std::fill_n(zero_revision.begin() + object_revision_offset, 8, 0);
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(decodePersistedTypeReferences(zero_revision)); });

    String path_dictionary_version = encoded;
    constexpr size_t path_dictionary_version_offset = descriptor_count_offset - 2;
    path_dictionary_version[path_dictionary_version_offset] = 2;
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion,
        [&] { static_cast<void>(decodePersistedTypeReferences(path_dictionary_version)); });

    String extension = encoded;
    extension[extension.size() - 4] = 2;
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion, [&] { static_cast<void>(decodePersistedTypeReferences(extension)); });

    String extension_flags = encoded;
    extension_flags[extension_flags.size() - 2] = 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion,
        [&] { static_cast<void>(decodePersistedTypeReferences(extension_flags)); });

    String nonminimal_count = encoded;
    nonminimal_count.replace(descriptor_count_offset, 1, String{"\x82\x00", 2});
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical, [&] { static_cast<void>(decodePersistedTypeReferences(nonminimal_count)); });
}

TEST(UDTPersistedTypeReferences, RejectsMalformedDescriptorAndPathFrames)
{
    const String encoded = encodePersistedTypeReferences(representativeReferences());
    const auto descriptor_frames = dictionaryFrames(encoded, descriptor_count_offset);
    ASSERT_EQ(descriptor_frames.size(), 2);

    String descriptor_version = encoded;
    descriptor_version[descriptor_frames[0].payload_begin] = 2;
    expectReferencesError(
        PersistedTypeReferencesError::Code::UnsupportedVersion,
        [&] { static_cast<void>(decodePersistedTypeReferences(descriptor_version)); });

    size_t descriptor_position = descriptor_frames[0].payload_begin + 2 + 16 + 16 + 8 + 32;
    String oversized_arguments = encoded;
    replaceVarUInt(oversized_arguments, descriptor_position, (64ULL << 10) + 1);
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded,
        [&] { static_cast<void>(decodePersistedTypeReferences(oversized_arguments)); });

    static_cast<void>(readVarUInt(encoded, descriptor_position));
    descriptor_position += oneBinaryStringArgument().size();
    const UInt64 physical_type_size = readVarUInt(encoded, descriptor_position);
    descriptor_position += static_cast<size_t>(physical_type_size);
    const size_t instantiation_hash_offset = descriptor_position;

    String wrong_instantiation_hash = encoded;
    wrong_instantiation_hash[instantiation_hash_offset] ^= 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::DigestMismatch,
        [&] { static_cast<void>(decodePersistedTypeReferences(wrong_instantiation_hash)); });

    const size_t checker_abi_offset = instantiation_hash_offset + 32 + 32;
    String unsupported_abi = encoded;
    unsupported_abi[checker_abi_offset] = 3;
    unsupported_abi[checker_abi_offset + 1] = 0;
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(decodePersistedTypeReferences(unsupported_abi)); });

    const size_t capabilities_offset = checker_abi_offset + 8 + 32;
    String unknown_capability = encoded;
    unknown_capability[capabilities_offset] = static_cast<char>(0x80);
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(decodePersistedTypeReferences(unknown_capability)); });

    const size_t name_length_offset = capabilities_offset + 1;
    size_t name_payload_offset = name_length_offset;
    const UInt64 name_size = readVarUInt(encoded, name_payload_offset);
    ASSERT_GT(name_size, 1);
    String descriptor_trailing = encoded;
    replaceVarUInt(descriptor_trailing, name_length_offset, name_size - 1);
    expectReferencesError(
        PersistedTypeReferencesError::Code::TrailingData, [&] { static_cast<void>(decodePersistedTypeReferences(descriptor_trailing)); });

    const size_t path_count_offset = nextDictionaryOffset(descriptor_frames);
    const auto path_frames = dictionaryFrames(encoded, path_count_offset);
    ASSERT_EQ(path_frames.size(), 3);
    String unknown_section = encoded;
    unknown_section[path_frames[0].payload_begin] = 9;
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(decodePersistedTypeReferences(unknown_section)); });

    String excessive_depth = encoded;
    excessive_depth[path_frames[0].payload_begin + 3] = 65;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(decodePersistedTypeReferences(excessive_depth)); });
}

TEST(UDTPersistedTypeReferences, RejectsUnsortedAndDuplicateDictionaries)
{
    const String encoded = encodePersistedTypeReferences(representativeReferences());
    const auto descriptor_frames = dictionaryFrames(encoded, descriptor_count_offset);
    ASSERT_EQ(descriptor_frames.size(), 2);

    String unsorted_descriptors = encoded;
    const String first_descriptor
        = encoded.substr(descriptor_frames[0].frame_begin, descriptor_frames[0].frameEnd() - descriptor_frames[0].frame_begin);
    const String second_descriptor
        = encoded.substr(descriptor_frames[1].frame_begin, descriptor_frames[1].frameEnd() - descriptor_frames[1].frame_begin);
    unsorted_descriptors.replace(
        descriptor_frames[0].frame_begin,
        descriptor_frames[1].frameEnd() - descriptor_frames[0].frame_begin,
        second_descriptor + first_descriptor);
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(decodePersistedTypeReferences(unsorted_descriptors)); });

    String duplicate_descriptors = encoded;
    duplicate_descriptors.replace(
        descriptor_frames[1].frame_begin, descriptor_frames[1].frameEnd() - descriptor_frames[1].frame_begin, first_descriptor);
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(decodePersistedTypeReferences(duplicate_descriptors)); });

    auto diagnostic_duplicate = representativeReferences();
    const auto & descriptor = diagnostic_duplicate.descriptors.front();
    auto renamed = PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        descriptor.getDefinitionIdentity(),
        descriptor.getDefinitionHash(),
        descriptor.getCanonicalArgumentsEncoding(),
        descriptor.getCanonicalPhysicalType(),
        descriptor.getInstantiationSemanticHash(),
        descriptor.getStorageFingerprint(),
        descriptor.getCheckerABI(),
        descriptor.getCheckerChargeABI(),
        descriptor.getPolicyABI(),
        descriptor.getFunctionRegistryABI(),
        descriptor.getPolicySemanticHash(),
        descriptor.getSemanticCapabilities(),
        "db.Renamed");
    diagnostic_duplicate.descriptors = {descriptor, std::move(renamed)};
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(encodePersistedTypeReferences(diagnostic_duplicate)); });

    const size_t path_count_offset = nextDictionaryOffset(descriptor_frames);
    const auto path_frames = dictionaryFrames(encoded, path_count_offset);
    ASSERT_EQ(path_frames.size(), 3);
    const String first_path = encoded.substr(path_frames[0].frame_begin, path_frames[0].frameEnd() - path_frames[0].frame_begin);
    const String second_path = encoded.substr(path_frames[1].frame_begin, path_frames[1].frameEnd() - path_frames[1].frame_begin);

    String unsorted_paths = encoded;
    unsorted_paths.replace(path_frames[0].frame_begin, path_frames[1].frameEnd() - path_frames[0].frame_begin, second_path + first_path);
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical, [&] { static_cast<void>(decodePersistedTypeReferences(unsorted_paths)); });

    String duplicate_paths = encoded;
    duplicate_paths.replace(path_frames[1].frame_begin, path_frames[1].frameEnd() - path_frames[1].frame_begin, first_path);
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical, [&] { static_cast<void>(decodePersistedTypeReferences(duplicate_paths)); });
}

TEST(UDTPersistedTypeReferences, EnforcesExactPathCoverageAndDescriptorReachability)
{
    const auto valid = representativeReferences();

    auto missing_use = valid;
    missing_use.uses.pop_back();
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(encodePersistedTypeReferences(missing_use)); });

    auto duplicate_path_id = valid;
    duplicate_path_id.uses[1].path_id = 0;
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical, [&] { static_cast<void>(encodePersistedTypeReferences(duplicate_path_id)); });

    auto absent_descriptor = valid;
    absent_descriptor.uses[1].descriptor_id = absent_descriptor.descriptors.size();
    expectReferencesError(
        PersistedTypeReferencesError::Code::InvalidValue, [&] { static_cast<void>(encodePersistedTypeReferences(absent_descriptor)); });

    auto unreferenced_descriptor = valid;
    for (auto & use : unreferenced_descriptor.uses)
        use.descriptor_id = 0;
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(encodePersistedTypeReferences(unreferenced_descriptor)); });

    auto duplicate_occurrence = valid;
    duplicate_occurrence.occurrence_paths[1] = duplicate_occurrence.occurrence_paths[0];
    expectReferencesError(
        PersistedTypeReferencesError::Code::NonCanonical,
        [&] { static_cast<void>(encodePersistedTypeReferences(duplicate_occurrence)); });
}

TEST(UDTPersistedTypeReferences, EnforcesProspectiveCountDepthFieldAndTotalByteLimits)
{
    const auto references = representativeReferences();
    const String encoded = encodePersistedTypeReferences(references);

    PersistedTypeReferencesLimits limits;
    limits.maximum_descriptors = 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(encodePersistedTypeReferences(references, limits)); });

    limits = {};
    limits.maximum_occurrence_paths = 2;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(encodePersistedTypeReferences(references, limits)); });

    limits = {};
    limits.maximum_path_depth = 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(encodePersistedTypeReferences(references, limits)); });

    limits = {};
    limits.maximum_canonical_physical_type_bytes = 5;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(encodePersistedTypeReferences(references, limits)); });

    limits = {};
    limits.maximum_sidecar_bytes = encoded.size() - 1;
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(encodePersistedTypeReferences(references, limits)); });
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(decodePersistedTypeReferences(encoded, limits)); });

    String impossible_count = encoded;
    replaceVarUInt(impossible_count, descriptor_count_offset, 65'536);
    expectReferencesError(
        PersistedTypeReferencesError::Code::Truncated, [&] { static_cast<void>(decodePersistedTypeReferences(impossible_count)); });

    String excessive_count = encoded;
    replaceVarUInt(excessive_count, descriptor_count_offset, 65'537);
    expectReferencesError(
        PersistedTypeReferencesError::Code::LimitExceeded, [&] { static_cast<void>(decodePersistedTypeReferences(excessive_count)); });
}

}
}
