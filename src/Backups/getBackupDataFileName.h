#pragma once

#include <Backups/BackupDataFileNameGeneratorType.h>
#include <Backups/BackupFileInfo.h>

namespace DB
{

/**
 * Generates a backup data file name based on the specified generator type.
 *
 * If `data_file_name_generator` is `Checksum`, the file name is derived from
 * `file_info.checksum` as a lowercase hex string. When `prefix_length` > 0 and
 * smaller than the checksum length, the name is split into `<prefix>/<suffix>`.
 * Otherwise, the checksum string is returned as-is.
 *
 * For other generator types, returns `file_info.file_name`.
 *
 * @throws DB::Exception If checksum is zero for `Checksum` type.
 */
std::string getBackupDataFileName(const BackupFileInfo & file_info, BackupDataFileNameGeneratorType generator, size_t prefix_length);
}
