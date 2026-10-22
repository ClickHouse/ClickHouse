#include <Storages/ObjectStorageQueue/ObjectStorageQueueMoveDestinationAccess.h>

#include "config.h"

#include <Access/Common/AccessType.h>
#include <Common/RemoteHostFilter.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <IO/S3/URI.h>

#include <Poco/URI.h>

#include <algorithm>
#include <optional>
#include <string_view>


namespace DB
{

namespace
{

enum class MoveDestinationKind : uint8_t
{
    S3,
    Azure,
};

std::optional<MoveDestinationKind> getMoveDestinationKind(std::string_view name)
{
    /// The prefix is accepted for every queue setting, see `normalizeSetting` in `StorageObjectStorageQueue.cpp`.
    static constexpr std::string_view legacy_prefix = "s3queue_";
    if (name.starts_with(legacy_prefix))
        name.remove_prefix(legacy_prefix.size());

    if (name == "after_processing_move_uri")
        return MoveDestinationKind::S3;
    if (name == "after_processing_move_connection_string" || name == "after_processing_move_container")
        return MoveDestinationKind::Azure;
    return std::nullopt;
}

/// A value that is not a `String` is never empty here: it is checked like a destination that cannot be parsed.
bool isEmptyValue(const Field & value)
{
    return value.getType() == Field::Types::String && value.safeGet<String>().empty();
}

/// A table moves files only to the destination of its own storage type, see `ObjectStorageQueuePostProcessor::process`.
AccessRightsElements getMoveDestinationAccess(const SettingsChanges & changes, MoveDestinationKind table_kind)
{
    const bool sets_destination = std::ranges::any_of(
        changes,
        [&](const SettingChange & change) { return getMoveDestinationKind(change.name) == table_kind && !isEmptyValue(change.value); });
    if (!sets_destination)
        return {};

    /// Unfiltered on purpose: a move has no single destination path to match a filter against.
    /// For S3, the move writes keys built from `after_processing_move_prefix` and the source keys, and uses only
    /// the bucket of the URI, not its key. For Azure, the `azureBlobStorage` function matches source filters against
    /// the blob path (`StorageAzureConfiguration::getRawURI` returns `blob_path.path`).
    const auto source = table_kind == MoveDestinationKind::S3 ? AccessTypeObjects::Source::S3 : AccessTypeObjects::Source::AZURE;
    AccessRightsElements res;
    res.emplace_back(AccessType::WRITE, AccessTypeObjects::toStringSource(source));
    return res;
}

}

AccessRightsElements getS3QueueMoveDestinationAccess(const SettingsChanges & changes)
{
    return getMoveDestinationAccess(changes, MoveDestinationKind::S3);
}

AccessRightsElements getAzureQueueMoveDestinationAccess(const SettingsChanges & changes)
{
    return getMoveDestinationAccess(changes, MoveDestinationKind::Azure);
}

void checkObjectStorageQueueS3MoveDestinationHost(
    [[maybe_unused]] const String & move_uri,
    [[maybe_unused]] const RemoteHostFilter & filter)
{
#if USE_AWS_S3
    if (!move_uri.empty())
        S3::URI(move_uri).checkRemoteHostFilter(filter);
#endif
}

void checkObjectStorageQueueAzureMoveDestinationHost(
    [[maybe_unused]] const String & move_connection_string,
    [[maybe_unused]] const String & move_container,
    [[maybe_unused]] const RemoteHostFilter & filter)
{
#if USE_AZURE_BLOB_STORAGE
    if (!move_connection_string.empty())
    {
        AzureBlobStorage::ConnectionParams connection_params;
        AzureBlobStorage::processURL(move_connection_string, move_container, connection_params.endpoint, connection_params.auth_method);
        filter.checkURL(Poco::URI(connection_params.getConnectionURLWithoutQuery()));
    }
#endif
}

}
