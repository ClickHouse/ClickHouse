#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/Record.h>

#include <gtest/gtest.h>

#include <limits>
#include <string_view>
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

Record representativeRecord()
{
    Record record;
    record.identity = {
        .database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL),
        .type_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe0fULL),
        .revision = 7,
    };
    record.normalized_name = "app.NestedValue";
    record.normalized_local_name = "NestedValue";
    record.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt16},
    };
    record.decreasing_parameter = 1;
    record.checker_abi = 2;
    record.policy_semantic_hash = CheckerProof::empty_policy_semantic_hash;
    record.canonical_definition_sql = "CREATE TYPE app.NestedValue(T TYPE, N UInt16) DECREASES N AS UInt64";
    record.canonical_physical_template_sql = "UInt64";
    record.canonical_template_ir = String{"ir\0v3", 5};
    record.dependencies = {{.type_uuid = uuid(0x20, 0x30), .revision = 9, .target_definition_hash = digest(0x30)}};
    record.semantic_definition_digest = digest(0x50);
    record.definition_hash = digest(0x70);
    record.compositional_dependency_closure_digest = digest(0x90);
    record.encoded_checker_certificate = String{"certificate\0v2", 14};
    record.checker_certificate_digest = hashDomainSeparated(CheckerProof::checker_proof_domain, record.encoded_checker_certificate);
    record.charged_work = 101;
    record.logical_node_count = 7;
    record.maximum_template_depth = 3;
    record.owner_uuid = uuid(0x40, 0x50);
    record.owner_display_name = "alice";
    record.comment = "comment";
    record.creation_time_us_utc = 123'456;
    return record;
}

Definition::Ptr checkedDefinition()
{
    DefinitionInput input;
    input.identity = {.database_uuid = uuid(1, 2), .type_uuid = uuid(3, 4), .revision = 1};
    input.normalized_name = "Value";
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return TemplateChecker::checkAll({std::move(input)}).front();
}

template <typename Callback>
void expectRecordError(RecordError::Code expected, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected RecordError";
    }
    catch (const RecordError & error)
    {
        EXPECT_EQ(error.code, expected);
    }
}

TEST(UDTRecord, CodecAndHashAreStable)
{
    const auto record = representativeRecord();
    const String encoded = encodeRecord(record);
    EXPECT_EQ(encoded.size(), 470);
    EXPECT_EQ(toHex(computeRecordHash(record)), "eaf8c7c64e1e55cabf30c5d5ece0c0d083ede98ba1cead2d05e37ad65c69c123");
    EXPECT_EQ(decodeRecord(encoded), record);
}

TEST(UDTRecord, CheckedDefinitionAndAdministrativeMetadataStaySeparate)
{
    const auto definition = checkedDefinition();
    RecordMetadata metadata{
        .canonical_definition_sql = "CREATE TYPE Value AS UInt64",
        .canonical_physical_template_sql = "UInt64",
        .owner_uuid = uuid(5, 6),
        .owner_display_name = "alice",
        .comment = "first",
        .creation_time_us_utc = 123,
    };
    auto record = makeRecord(*definition, metadata);
    EXPECT_EQ(record.maximum_template_depth, 0);
    EXPECT_TRUE(recordMatchesCheckedDefinition(record, *definition));

    const Digest first_hash = computeRecordHash(record);
    record.comment = "second";
    EXPECT_EQ(record.definition_hash, definition->getDefinitionHash());
    EXPECT_TRUE(recordMatchesCheckedDefinition(record, *definition));
    EXPECT_NE(computeRecordHash(record), first_hash);

    record.definition_hash[0] ^= 0x80;
    EXPECT_FALSE(recordMatchesCheckedDefinition(record, *definition));
}

TEST(UDTRecord, RejectsMalformedNoncanonicalAndOversizedRecords)
{
    const auto record = representativeRecord();
    const String encoded = encodeRecord(record);

    expectRecordError(
        RecordError::Code::Truncated,
        [&] { static_cast<void>(decodeRecord(std::string_view(encoded).substr(0, encoded.size() - 1))); });

    String trailing = encoded;
    trailing.push_back('\0');
    expectRecordError(RecordError::Code::TrailingData, [&] { static_cast<void>(decodeRecord(trailing)); });

    String unsupported = encoded;
    unsupported[0] = 2;
    expectRecordError(
        RecordError::Code::UnsupportedVersion, [&] { static_cast<void>(decodeRecord(unsupported)); });

    auto duplicate_parameters = record;
    duplicate_parameters.parameters[1].normalized_name = duplicate_parameters.parameters[0].normalized_name;
    expectRecordError(
        RecordError::Code::InvalidValue, [&] { static_cast<void>(encodeRecord(duplicate_parameters)); });

    auto empty_owner = record;
    empty_owner.owner_display_name.clear();
    expectRecordError(
        RecordError::Code::InvalidValue, [&] { static_cast<void>(encodeRecord(empty_owner)); });

    auto bad_certificate_digest = record;
    bad_certificate_digest.checker_certificate_digest[0] ^= 0x80;
    expectRecordError(
        RecordError::Code::InvalidValue, [&] { static_cast<void>(encodeRecord(bad_certificate_digest)); });

    auto acyclic_with_measure = record;
    acyclic_with_measure.checker_abi = 1;
    expectRecordError(
        RecordError::Code::InvalidValue, [&] { static_cast<void>(encodeRecord(acyclic_with_measure)); });

    auto recursive_policy = record;
    recursive_policy.policy_bearing = true;
    recursive_policy.policy_semantic_hash = digest(1);
    expectRecordError(
        RecordError::Code::InvalidValue, [&] { static_cast<void>(encodeRecord(recursive_policy)); });

    auto inconsistent_empty_policy = record;
    inconsistent_empty_policy.policy_semantic_hash = digest(1);
    expectRecordError(
        RecordError::Code::InvalidValue,
        [&] { static_cast<void>(encodeRecord(inconsistent_empty_policy)); });

    auto unordered_dependencies = record;
    unordered_dependencies.dependencies.insert(
        unordered_dependencies.dependencies.begin(), {.type_uuid = uuid(0x30, 0x40), .revision = 1, .target_definition_hash = digest(1)});
    expectRecordError(
        RecordError::Code::NonCanonical, [&] { static_cast<void>(encodeRecord(unordered_dependencies)); });

    RecordLimits short_record;
    short_record.maximum_record_bytes = encoded.size() - 1;
    expectRecordError(
        RecordError::Code::LimitExceeded, [&] { static_cast<void>(encodeRecord(record, short_record)); });

    auto before_epoch = record;
    before_epoch.creation_time_us_utc = std::numeric_limits<Int64>::min();
    EXPECT_EQ(decodeRecord(encodeRecord(before_epoch)), before_epoch);
}

}
}
