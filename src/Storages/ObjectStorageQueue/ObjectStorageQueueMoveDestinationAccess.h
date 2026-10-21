#pragma once

#include <Common/SettingsChanges.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class RemoteHostFilter;

/// The move destination of an `S3Queue` or `AzureQueue` table is the same data flow as
/// `INSERT INTO FUNCTION s3('<destination>') SELECT * FROM <table>`: the table reads every processed file
/// with its own source credentials and writes it to the destination. So setting the destination needs the
/// same grants as that `INSERT`: `WRITE` on the `S3` (or `AZURE`) source for the destination. For `ALTER`
/// it also needs `SELECT` on the table, because the creator of the table, not the user of the `ALTER`,
/// supplied the source and its credentials.

/// Whether any of the changes sets a move destination. Only these settings choose a destination outside the source
/// bucket or container: `after_processing_move_uri` for S3 and `after_processing_move_connection_string` for Azure,
/// with or without the legacy `s3queue_` prefix. Changing only the keys or the container keeps the destination that
/// an authorized user set. An empty value disables the move destination, so it is not a destination and needs no grant.
bool hasObjectStorageQueueMoveDestination(const SettingsChanges & changes);

/// Throws `ACCESS_DENIED` if the current user has no `WRITE` grant on the source of a move destination in the changes.
/// It uses only the names and the values of the changes, so the table does not have to exist on this host.
void checkObjectStorageQueueMoveDestinationAccess(const SettingsChanges & changes, const ContextPtr & context);

/// Throws `UNACCEPTABLE_URL` if `remote_url_allow_hosts` does not allow the destination of a move to another S3 bucket.
/// The move builds its client directly, so the destination never passes `StorageS3Configuration::check`.
/// Does nothing when `move_uri` is empty.
void checkObjectStorageQueueS3MoveDestinationHost(const String & move_uri, const RemoteHostFilter & filter);

/// The same for a move to another Azure container (see `StorageAzureConfiguration::check`).
/// Does nothing when `move_connection_string` is empty.
void checkObjectStorageQueueAzureMoveDestinationHost(
    const String & move_connection_string, const String & move_container, const RemoteHostFilter & filter);

}
