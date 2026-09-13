#pragma once

#include <Core/Types.h>

#include <map>


namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class ReadBuffer;
class SeekableReadBuffer;
class ReadBufferFromFileBase;
class WriteBuffer;
enum class WriteMode : uint8_t;
struct WriteSettings;
struct ReadSettings;

/// Represents operations of loading from disk or downloading for reading a backup.
/// See also implementations: BackupReaderFile, BackupReaderDisk.
class IBackupReader
{
public:
    virtual ~IBackupReader() = default;

    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;

    /// `expected_file_size` is the size the backup metadata recorded for the file, when the caller
    /// knows it. A reader whose object storage can have the blob replaced under it (Azure) refuses
    /// to read a blob of another size, because such a blob is not the one the backup wrote; the
    /// others ignore it. Nothing is passed for a read whose size is not recorded anywhere, such as
    /// the `.backup` metadata file itself.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name, std::optional<size_t> expected_file_size) = 0;

    /// Names the generation of `file_name` that is in the storage now, for a reader whose files can
    /// be replaced under an open backup (Azure, where a blob is rewritten in place, and S3, where an
    /// object of an unversioned bucket is). A backup read through several buffers - an archive,
    /// which is reopened for every handle the archive reader needs - takes this token once and
    /// passes it to every one of those reads, so that the whole session reads one generation of the
    /// archive or fails, instead of taking whatever generation each reopen is answered with. Empty
    /// where a file cannot change identity under an open backup, which is also the case of an S3
    /// URI that names a version: such a read is pinned by the version itself.
    virtual String getFileGeneration(const String & /*file_name*/) { return {}; }

    /// Reads `file_name` pinned to the generation named by `generation` (a token of
    /// getFileGeneration()): a file that does not hold that generation any more is refused rather
    /// than read - with `FILE_CHANGED_DURING_READ` on Azure and `S3_OBJECT_CHANGED_DURING_READ` on
    /// S3, whose `If-Match` failure has a code of its own. An empty token pins nothing, which is
    /// what a reader of a storage where a file cannot be replaced in place has to offer.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFilePinnedToGeneration(
        const String & file_name, std::optional<size_t> expected_file_size, const String & generation);

    /// The function copyFileToDisk() can be much faster than reading the file with readFile() and then writing it to some disk.
    /// (especially for S3 where it can use CopyObject to copy objects inside S3 instead of downloading and uploading them).
    /// Parameters:
    /// `encrypted_in_backup` specify if this file is encrypted in the backup, so it shouldn't be encrypted again while restoring to an encrypted disk.
    virtual void copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) = 0;

    /// Copies exactly `[offset, offset + size)` of `path_in_backup`, whose full size is `file_size`.
    /// Separate from copyFileToDisk() because the whole-object fast paths (a server-side copy, fs::copy) carry
    /// no byte range and would copy the entire file. `file_size` lets an implementation choose a route the
    /// storage allows for that source (see copyS3FileRange) without an extra metadata request.
    virtual void copyFileRangeToDisk(const String & path_in_backup, size_t offset, size_t size, size_t file_size,
                                     bool encrypted_in_backup, DiskPtr destination_disk, const String & destination_path,
                                     WriteMode write_mode) = 0;

    virtual const ReadSettings & getReadSettings() const = 0;
    virtual const WriteSettings & getWriteSettings() const = 0;
    virtual size_t getWriteBufferSize() const = 0;

    /// Settings effectively used by this reader (e.g. S3 request settings). Empty if none.
    virtual std::map<String, String> getSerializedSettings() const { return {}; }
};

/// Represents operations of storing to disk or uploading for writing a backup.
/// See also implementations: BackupWriterFile, BackupWriterDisk
class IBackupWriter
{
public:
    virtual ~IBackupWriter() = default;

    virtual bool fileExists(const String & file_name) = 0;
    virtual UInt64 getFileSize(const String & file_name) = 0;
    virtual bool fileContentsEqual(const String & file_name, const String & expected_file_contents, String & actual_file_contents) = 0;
    virtual std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) = 0;

    virtual std::unique_ptr<WriteBuffer> writeFile(const String & file_name) = 0;
    /// Object-storage writers override this to create a file atomically without replacing an existing one.
    virtual std::unique_ptr<WriteBuffer> writeFileIfNotExists(const String & file_name);

    using CreateReadBufferFunction = std::function<std::unique_ptr<SeekableReadBuffer>()>;
    virtual void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) = 0;

    /// The function copyFileFromDisk() can be much faster than copyDataToFile()
    /// (especially for S3 where it can use CopyObject to copy objects inside S3 instead of downloading and uploading them).
    /// Parameters:
    /// `start_pos` and `length` specify a part of the file on `src_disk` to copy to the backup.
    /// `copy_encrypted` specify whether this function should copy encrypted data of the file `src_path` to the backup.
    virtual void copyFileFromDisk(
        const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
        = 0;

    virtual void copyFile(const String & destination, const String & source, size_t size) = 0;

    /// Removes a file written to the backup, if it still exists.
    virtual void removeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;

    /// Removes the backup folder if it's empty or contains empty subfolders.
    virtual void removeEmptyDirectories() = 0;

    virtual const ReadSettings & getReadSettings() const = 0;
    virtual const WriteSettings & getWriteSettings() const = 0;
    virtual size_t getWriteBufferSize() const = 0;

    /// Settings effectively used by this writer (e.g. S3 request settings). Empty if none.
    virtual std::map<String, String> getSerializedSettings() const { return {}; }
};

}
