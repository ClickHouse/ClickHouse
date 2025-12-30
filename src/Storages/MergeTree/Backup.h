#pragma once

#include <optional>
#include <base/types.h>
#include <Disks/IDisk.h>

namespace DB
{

struct WriteSettings;

/** Creates a local (at the same mount point) backup (snapshot) directory.
  *
  * In the specified destination directory, it creates hard links on all source-directory files
  *  and in all nested directories, with saving (creating) all relative paths;
  *  and also `chown`, removing the write permission.
  *
  * This protects data from accidental deletion or modification,
  *  and is intended to be used as a simple means of protection against a human or program error,
  *  but not from a hardware failure.
  *
  * If max_level is specified, than only files with depth relative source_path less or equal max_level will be copied.
  *  So, if max_level=0 than only direct file child are copied.
  *
  * If `transaction` is provided, the changes will be added to it instead of performend on disk.
  */
    void Backup(
        const DiskPtr & src_disk,
        const DiskPtr & dst_disk,
        const String & source_path,
        const String & destination_path,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        bool make_source_readonly = true,
        std::optional<size_t> max_level = {},
        bool copy_instead_of_hardlinks = false,
        const NameSet & files_to_copy_intead_of_hardlinks = {},
        DiskTransactionPtr disk_transaction = nullptr);

}
