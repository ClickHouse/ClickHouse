#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <Common/SettingsChanges.h>

namespace DB
{

class RemoteHostFilter;

/// The move destination of an `S3Queue` or `AzureQueue` table is the same data flow as
/// `INSERT INTO FUNCTION s3('<destination>') SELECT * FROM <table>`: the table reads every processed file
/// with its own source credentials and writes it to the destination. So setting the destination needs
/// `WRITE` on the `S3` (or `AZURE`) source without a URL filter: the move writes keys built from
/// `after_processing_move_prefix` and the source keys, not from the URI, so a filtered grant cannot authorize them.
/// For `ALTER` it also needs `SELECT` on the table, because the creator of the table, not the user of the `ALTER`,
/// supplied the source and its credentials.

/// The `StorageFactory::GetExportAccessFn` of `S3Queue` and `AzureQueue`: unfiltered `WRITE` if a setting moves files outside
/// the source bucket or container. Only these settings choose such a destination: `after_processing_move_uri` for S3,
/// `after_processing_move_connection_string` and `after_processing_move_container` for Azure, with or without the legacy
/// `s3queue_` prefix. Changing only the move keys or prefix stays inside a destination an authorized user set.
/// An empty value disables the move destination, so it is not a destination and needs no grant.
AccessRightsElements getS3QueueMoveDestinationAccess(const SettingsChanges & changes);
AccessRightsElements getAzureQueueMoveDestinationAccess(const SettingsChanges & changes);

/// Throws `UNACCEPTABLE_URL` if `remote_url_allow_hosts` does not allow the destination of a move to another S3 bucket.
/// The move builds its client directly, so the destination never passes `StorageS3Configuration::check`.
/// Does nothing when `move_uri` is empty.
void checkObjectStorageQueueS3MoveDestinationHost(const String & move_uri, const RemoteHostFilter & filter);

/// The same for a move to another Azure container (see `StorageAzureConfiguration::check`).
/// Does nothing when `move_connection_string` is empty.
void checkObjectStorageQueueAzureMoveDestinationHost(
    const String & move_connection_string, const String & move_container, const RemoteHostFilter & filter);

}
