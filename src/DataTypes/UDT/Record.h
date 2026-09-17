#pragma once

#include <DataTypes/UDT/Definition.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 record_format_version = 1;

enum class StorageBackend : UInt8
{
    AtomicDisk = 1,
};

struct RecordMetadata
{
    String canonical_definition_sql;
    String canonical_physical_template_sql;
    UUID owner_uuid = UUIDHelpers::Nil;
    String owner_display_name;
    String comment;
    Int64 creation_time_us_utc = 0;
    StorageBackend storage_backend = StorageBackend::AtomicDisk;
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;

    bool operator==(const RecordMetadata &) const = default;
};

/// Durable authority wrapper. The canonical SQL strings are recovery and
/// presentation bytes, while owner/comment/time are administrative metadata;
/// neither group substitutes for freshly checked executable semantics.
struct Record
{
    UInt16 format_version = record_format_version;
    DefinitionIdentity identity;
    String normalized_name;
    String normalized_local_name;
    std::vector<Parameter> parameters;
    std::optional<UInt16> decreasing_parameter;
    UInt16 checker_abi = 1;
    UInt16 checker_charge_abi = 1;
    UInt16 policy_abi = 1;
    UInt16 function_registry_abi = 1;
    bool policy_bearing = false;
    SemanticCapabilityMask semantic_capabilities = 0;
    Digest policy_semantic_hash = CheckerProof::empty_policy_semantic_hash;
    String canonical_definition_sql;
    String canonical_physical_template_sql;
    String canonical_template_ir;
    std::vector<DefinitionDependency> dependencies;
    Digest semantic_definition_digest{};
    Digest definition_hash{};
    Digest compositional_dependency_closure_digest{};
    String encoded_checker_certificate;
    Digest checker_certificate_digest{};
    UInt64 charged_work = 0;
    UInt64 logical_node_count = 0;
    UInt64 maximum_template_depth = 0;
    UUID owner_uuid = UUIDHelpers::Nil;
    String owner_display_name;
    String comment;
    Int64 creation_time_us_utc = 0;
    StorageBackend storage_backend = StorageBackend::AtomicDisk;
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;

    bool operator==(const Record &) const = default;
};

struct RecordLimits
{
    UInt64 maximum_record_bytes = 1ULL << 20;
    UInt64 maximum_name_bytes = 4ULL << 10;
    UInt64 maximum_parameter_count = 64;
    UInt64 maximum_parameter_name_bytes = 1ULL << 10;
    UInt64 maximum_canonical_sql_bytes = 256ULL << 10;
    UInt64 maximum_template_ir_bytes = 256ULL << 10;
    UInt64 maximum_dependency_count = 256;
    UInt64 maximum_checker_certificate_bytes = 256ULL << 10;
    UInt64 maximum_owner_display_name_bytes = 4ULL << 10;
    UInt64 maximum_comment_bytes = 256ULL << 10;
};

class RecordError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
    };

    RecordError(Code code_, std::string_view message);

    const Code code;
};

Record makeRecord(
    const Definition & definition,
    RecordMetadata metadata,
    const RecordLimits & limits = {});

String encodeRecord(const Record & record, const RecordLimits & limits = {});
Record decodeRecord(std::string_view bytes, const RecordLimits & limits = {});
Digest computeRecordHash(const Record & record, const RecordLimits & limits = {});

/// Called after metadata SQL is lowered and TemplateChecker::checkAll returns.
/// The comparison is complete; no stored digest substitutes for checked bytes.
bool recordMatchesCheckedDefinition(
    const Record & record, const Definition & definition) noexcept;

}
