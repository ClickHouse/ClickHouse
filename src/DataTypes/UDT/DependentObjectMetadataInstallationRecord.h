#pragma once

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>

#include <Core/Types.h>

#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 dependent_object_metadata_installation_record_format_version = 1;

/// Durable name-to-artifact mapping for one exact dependent-object revision.
/// Filesystem escaping and database paths are deliberately derived by the
/// owning database and never become part of these canonical bytes.
struct DependentObjectMetadataInstallationRecord
{
    UInt16 format_version = dependent_object_metadata_installation_record_format_version;
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    String object_name;
    Digest metadata_artifact_hash{};
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;

    bool operator==(const DependentObjectMetadataInstallationRecord &) const = default;
};

struct DependentObjectMetadataInstallationRecordLimits
{
    UInt64 maximum_encoded_bytes = 64ULL << 10;
    UInt64 maximum_object_name_bytes = 4ULL << 10;
};

class DependentObjectMetadataInstallationRecordError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
    };

    DependentObjectMetadataInstallationRecordError(Code code_, std::string_view message);

    const Code code;
};

String encodeDependentObjectMetadataInstallationRecord(
    const DependentObjectMetadataInstallationRecord & record, const DependentObjectMetadataInstallationRecordLimits & limits = {});

DependentObjectMetadataInstallationRecord decodeDependentObjectMetadataInstallationRecord(
    std::string_view bytes, const DependentObjectMetadataInstallationRecordLimits & limits = {});

Digest computeDependentObjectMetadataInstallationRecordHash(
    const DependentObjectMetadataInstallationRecord & record, const DependentObjectMetadataInstallationRecordLimits & limits = {});

}
