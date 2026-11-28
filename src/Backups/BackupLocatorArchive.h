#pragma once

#include <Backups/BackupFactory.h>
#include <Backups/BackupImpl.h>

namespace DB
{

/// Splits the last path component off `url` and returns it, leaving in `url` the directory part with its
/// trailing slash, as re-serialized by `Poco::URI`. A path without a slash is all file name.
String removeFileNameFromURL(String & url);

/// The archive parameters of a backup whose locator names the archive `archive_name`, or empty parameters
/// when `archive_name` is empty. Throws if an archive is used by a host's part of an `ON CLUSTER` backup
/// (`is_internal_backup`), or a password is given for a backup which is not an archive.
/// The archive formats an engine rejects are checked by its caller before calling it.
BackupImpl::ArchiveParams makeBackupArchiveParams(const BackupFactory::CreateParams & params, const String & archive_name);

}
