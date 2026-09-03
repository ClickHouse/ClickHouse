#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

void PersistentTableComponents::checkTableWasNotReplaced(std::optional<UInt64> validated_incarnation, std::string_view operation) const
{
    if (validated_incarnation.has_value() && *validated_incarnation != trusted_table_uuid->getIncarnation())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The Iceberg table at {} was replaced while {} was running. Retry the statement.",
            table_path,
            operation);
}

void PersistentTableComponents::checkMetadataBelongsToValidatedTable(
    const Poco::JSON::Object::Ptr & metadata_object,
    std::optional<UInt64> validated_incarnation,
    std::string_view operation) const
{
    if (!validated_incarnation.has_value())
        return;

    /// The UUID of the validated incarnation, which is `std::nullopt` once the shared cell has
    /// moved past it - a replacement that was already observed, reported by the check below.
    const auto validated_uuid = trusted_table_uuid->getForPinnedIncarnation(validated_incarnation);
    checkTableWasNotReplaced(validated_incarnation, operation);

    if (!validated_uuid.has_value() || !metadata_object->has(f_table_uuid))
        return;

    const auto metadata_uuid = normalizeUuid(metadata_object->getValue<String>(f_table_uuid));
    if (metadata_uuid != *validated_uuid)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The Iceberg table at {} was replaced while {} was running: the metadata now in storage describes table {}, "
            "while the statement was validated against table {}. Retry the statement.",
            table_path,
            operation,
            metadata_uuid,
            *validated_uuid);
}

void PersistentTableComponents::checkMetadataMatchesPinnedState(
    const Poco::JSON::Object::Ptr & metadata_object, std::optional<UInt64> pinned_token, std::string_view operation) const
{
    if (!pinned_token.has_value())
        return;

    if (computeMetadataContentToken(metadata_object) != *pinned_token)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The Iceberg table at {} was replaced while {} was running: the metadata file the statement pinned no longer carries the "
            "content it was analysed with. Retry the statement.",
            table_path,
            operation);
}

UInt64 computeMetadataContentToken(const Poco::JSON::Object::Ptr & metadata_object)
{
    /// Every top-level field a commit of this table advances, plus the fields that identify the
    /// table itself. A file written by another table at this path differs in them.
    static constexpr std::array identifying_fields = {
        f_format_version,
        f_table_uuid,
        f_location,
        f_last_updated_ms,
        f_last_sequence_number,
        f_last_column_id,
        f_current_schema_id,
        f_current_snapshot_id,
        f_default_sort_order_id,
    };

    SipHash hash;
    for (const auto & field : identifying_fields)
    {
        hash.update(field);
        /// An absent and a null field are both "not set here", and the name that was hashed above
        /// already keeps them apart from the next field's value.
        if (!metadata_object->has(field) || metadata_object->isNull(field))
            continue;
        hash.update(metadata_object->getValue<String>(field));
    }
    return hash.get64();
}

bool metadataDescendsFromValidatedFile(
    const Poco::JSON::Object::Ptr & metadata_object, const String & metadata_object_path, const String & validated_path)
{
    /// The paths are written by whoever committed the file, so one of them can be a full location
    /// (`s3://bucket/table/metadata/v1.metadata.json`) while the other is the key within the
    /// bucket. Matching on the tail is what the two spellings of the same object share.
    auto is_same_file = [&](const String & path)
    { return path.ends_with(validated_path) || validated_path.ends_with(path); };

    if (is_same_file(metadata_object_path))
        return true;

    if (!metadata_object->has(f_metadata_log))
        return false;

    auto metadata_log = metadata_object->get(f_metadata_log).extract<Poco::JSON::Array::Ptr>();
    if (!metadata_log)
        return false;

    for (UInt32 i = 0; i < metadata_log->size(); ++i)
    {
        auto entry = metadata_log->getObject(i);
        if (entry && entry->has(f_metadata_file) && is_same_file(entry->getValue<String>(f_metadata_file)))
            return true;
    }

    return false;
}

}

#endif
