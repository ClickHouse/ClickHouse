#pragma once

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>

#include <Core/Types.h>

#include <optional>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 sidecar_expectation_record_format_version = 1;
inline constexpr size_t sidecar_expectation_record_encoded_bytes = 111;
inline constexpr size_t sidecar_expectation_record_extended_encoded_bytes = sidecar_expectation_record_encoded_bytes + sizeof(Digest);

/// Durable authority record binding one schema-object revision to the exact
/// sidecar and physical schema that must be installed with it.
struct SidecarExpectationRecord
{
    UInt16 format_version = sidecar_expectation_record_format_version;
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;
    /// Hash of the canonical name-to-metadata installation record. Its
    /// absence retains the byte-exact 111-byte base encoding.
    std::optional<Digest> installation_record_hash = std::nullopt;

    bool operator==(const SidecarExpectationRecord &) const = default;
};

class SidecarExpectationRecordError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        TrailingData,
    };

    SidecarExpectationRecordError(Code code_, std::string_view message);

    const Code code;
};

String encodeSidecarExpectationRecord(const SidecarExpectationRecord & record);
SidecarExpectationRecord decodeSidecarExpectationRecord(std::string_view bytes);
Digest computeSidecarExpectationRecordHash(const SidecarExpectationRecord & record);

}
