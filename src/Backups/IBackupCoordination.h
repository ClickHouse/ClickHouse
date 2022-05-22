#pragma once

#include <Core/Types.h>
#include <optional>


namespace DB
{

/// Keeps information about files contained in a backup.
class IBackupCoordination
{
public:
    virtual ~IBackupCoordination() = default;

    struct FileInfo
    {
        String file_name;

        UInt64 size = 0;
        UInt128 checksum{0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0};

        /// Name of the data file.
        String data_file_name;

        /// Suffix of an archive if the backup is stored as a series of archives.
        String archive_suffix;

        /// Position in the archive.
        UInt64 pos_in_archive = static_cast<UInt64>(-1);
    };

    /// Adds file information.
    /// If specified checksum+size are new for this IBackupContentsInfo the function sets `is_data_file_required`.
    virtual void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) = 0;

    void addFileInfo(const FileInfo & file_info)
    {
        bool is_data_file_required;
        addFileInfo(file_info, is_data_file_required);
    }

    /// Updates some fields (currently only `archive_suffix`) of a stored file's information.
    virtual void updateFileInfo(const FileInfo & file_info) = 0;

    virtual std::vector<FileInfo> getAllFileInfos() const = 0;
    virtual Strings listFiles(const String & prefix, const String & terminator) const = 0;

    using SizeAndChecksum = std::pair<UInt64, UInt128>;

    virtual std::optional<FileInfo> getFileInfo(const String & file_name) const = 0;
    virtual std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) const = 0;
    virtual std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) const = 0;

    /// Generates a new archive suffix, e.g. "001", "002", "003", ...
    virtual String getNextArchiveSuffix() = 0;

    /// Returns the list of all the archive suffixes which were generated.
    virtual Strings getAllArchiveSuffixes() const = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
