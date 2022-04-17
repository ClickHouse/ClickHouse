#pragma once

#include <Core/Types.h>


namespace DB
{

/// Keeps information about files contained in a backup.
class IBackupCoordination
{
public:
    struct FileInfo
    {
        String file_name;

        UInt64 size = 0;
        UInt128 checksum{0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0};

        /// Suffix of an archive if the backup is stored as a series of archives.
        String archive_suffix;

        /// Position in the archive.
        UInt64 pos_in_archive = static_cast<UInt64>(-1);
    };

    virtual ~IBackupCoordination() { }

    /// Adds file information.
    /// If a specified checksum is new for this IBackupContentsInfo the function sets `is_new_checksum`.
    virtual void addFileInfo(const FileInfo & file_info, bool & is_new_checksum) = 0;

    void addFileInfo(const FileInfo & file_info)
    {
        bool is_new_checksum;
        addFileInfo(file_info, is_new_checksum);
    }

    /// Updates some fields (currently only `archive_suffix`) of a stored file's information.
    virtual void updateFileInfo(const FileInfo & file_info) = 0;

    virtual std::vector<FileInfo> getAllFileInfos() = 0;
    virtual Strings listFiles(const String & prefix, const String & terminator) = 0;

    virtual std::optional<UInt128> getChecksumByFileName(const String & file_name) = 0;
    virtual std::optional<FileInfo> getFileInfoByChecksum(const UInt128 & checksum) = 0;
    virtual std::optional<FileInfo> getFileInfoByFileName(const String & file_name) = 0;

    /// Generates a new archive suffix, e.g. "001", "002", "003", ...
    virtual String getNextArchiveSuffix() = 0;

    /// Returns the list of all the archive suffixes which were generated.
    virtual Strings getAllArchiveSuffixes() = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
