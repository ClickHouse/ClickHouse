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

AccessTypeObjects::Source getAccessSource(MoveDestinationKind kind)
{
    switch (kind)
    {
        case MoveDestinationKind::S3:
            return AccessTypeObjects::Source::S3;
        case MoveDestinationKind::Azure:
            return AccessTypeObjects::Source::AZURE;
    }
}

/// A value that is not a `String` is never empty here: it is checked like a destination that cannot be parsed.
bool isEmptyValue(const Field & value)
{
    return value.getType() == Field::Types::String && value.safeGet<String>().empty();
}

bool changesSourceObjects(const SettingChange & change)
{
    /// `after_processing` other than `keep` modifies the source files.
    return (change.name == "after_processing" || change.name == "s3queue_after_processing")
        && !(change.value.getType() == Field::Types::String && change.value.safeGet<String>() == "keep");
}

/// A table moves files only to the destination of its own storage type, see `ObjectStorageQueuePostProcessor::process`.
AccessRightsElements getMoveDestinationAccess(
    const SettingsChanges & changes, MoveDestinationKind table_kind, bool check_source_access)
{
    const bool needs_write = std::ranges::any_of(
        changes,
        [&](const SettingChange & change)
        {
            return (getMoveDestinationKind(change.name) == table_kind && !isEmptyValue(change.value))
                || (check_source_access && changesSourceObjects(change));
        });
    if (!needs_write)
        return {};

    /// Unfiltered on purpose: a move has no single destination path to match a filter against.
    /// For `after_processing`, the URL of the table is not known here either.
    /// For S3, the move writes keys built from `after_processing_move_prefix` and the source keys, and uses only
    /// the bucket of the URI, not its key. For Azure, the `azureBlobStorage` function matches source filters against
    /// the blob path (`StorageAzureConfiguration::getRawURI` returns `blob_path.path`).
    const auto source = getAccessSource(table_kind);
    AccessRightsElements res;
    res.emplace_back(AccessType::WRITE, AccessTypeObjects::toStringSource(source));
    return res;
}

}

AccessRightsElements getS3QueueMoveDestinationAccess(const SettingsChanges & changes, bool check_source_access)
{
    return getMoveDestinationAccess(changes, MoveDestinationKind::S3, check_source_access);
}

AccessRightsElements getAzureQueueMoveDestinationAccess(const SettingsChanges & changes, bool check_source_access)
{
    return getMoveDestinationAccess(changes, MoveDestinationKind::Azure, check_source_access);
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
