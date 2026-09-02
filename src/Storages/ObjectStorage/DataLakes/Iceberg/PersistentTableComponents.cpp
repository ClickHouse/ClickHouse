#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

#if USE_AVRO

#include <Common/Exception.h>
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

}

#endif
