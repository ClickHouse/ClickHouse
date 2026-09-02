#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

#if USE_AVRO

#include <Common/Exception.h>

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

}

#endif
