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

    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name) = 0;

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

    /// Whether `writeFileIfNotExists` is a real conditional create: it either creates the file or fails,
    /// and it never replaces a file somebody else created. The default implementation falls back to
    /// `writeFile`, which overwrites, so only writers that override both may answer `true`. Callers that
    /// take the destination with a lock file rely on this: with an overwriting writer, finding one's own
    /// contents in the lock proves nothing, because the write could have destroyed a foreign lock.
    virtual bool supportsAtomicCreateIfNotExists() const { return false; }

    /// Whether `writeFileIfNotExists` creates the file exclusively already when it returns the buffer, before
    /// any of its contents are written (an `O_EXCL` open). If the write fails afterwards, the file left in the
    /// destination was created by this caller, even though it does not hold the contents the caller meant to
    /// write. It is not proof that the file is still the caller's: another writer reaching the same path in
    /// rewrite mode can have written over it since. Object-storage writers create the object only when the
    /// buffer is finalized, so they answer `false`.
    virtual bool createsFileOnOpenIfNotExists() const { return false; }

    using CreateReadBufferFunction = std::function<std::unique_ptr<SeekableReadBuffer>()>;
    virtual void copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length) = 0;

    /// Returns true only if the copy completed. A false result requires the caller to use `copyFileFromDiskWithFallback`.
    /// `start_pos` and `length` specify the source range; `copy_encrypted` selects encrypted source bytes.
    virtual bool copyFileFromDiskNatively(
        const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
        = 0;

    /// Completes a copy after the native attempt returned false. Disk destinations may use `IDisk::copyFile`,
    /// which can copy on the storage side or through buffers; the default implementation reads and writes through buffers.
    virtual void copyFileFromDiskWithFallback(
        const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
        = 0;

    /// The function copyFileFromBlobPath() provides a fast path for copying files between object storages
    /// using native copy operations (e.g., S3 CopyObject, Azure Copy Blob).
    /// Parameters:
    /// `src_disk` - the source disk (needed to get the object storage client)
    /// `src_blob_key` - the blob key/path in the source object storage
    /// `src_blob_namespace` - the bucket/container name in the source object storage
    /// `copy_encrypted` - whether to copy encrypted data
    /// `start_pos` and `length` - specify a part of the file to copy
    /// `src_object_size` - the size of the whole source blob, so that a copy of a part of it can be told
    ///                     from a copy of all of it (S3 copies a range and a whole object differently)
    /// `create_fallback_read_buffer` - used to read the data if the native copy fails midway
    ///                                 (the source disk must not be read instead: it may have been
    ///                                 repointed to a different namespace since `src_blob_key` was
    ///                                 recorded, and `src_blob_key` is not a path on the disk anyway)
    /// Returns true if the native copy was performed, false if it should fall back to buffer copy.
    virtual bool copyFileFromBlobPath(
        const String & path_in_backup,
        DiskPtr src_disk,
        const String & src_blob_key,
        const String & src_blob_namespace,
        bool copy_encrypted,
        UInt64 start_pos,
        UInt64 length,
        UInt64 src_object_size,
        const CreateReadBufferFunction & create_fallback_read_buffer)
        = 0;

    virtual void copyFile(const String & destination, const String & source, size_t size) = 0;

    /// Removes a file written to the backup, if it still exists.
    virtual void removeFile(const String & file_name) = 0;
    virtual void removeFiles(const Strings & file_names) = 0;

    /// Removes the backup folder if it's empty or contains empty subfolders.
    virtual void removeEmptyDirectories() = 0;

    /// fsync one already-written file and remember its parent directories for
    /// `syncDirectoriesToDisk`. No-op where a completed upload is already durable.
    virtual void syncFileToDisk(const String & /* file_name */) {}

    /// fsync the directories recorded by `syncFileToDisk`: a created file is durable only once
    /// its parent directory entry is persisted. Called once after all files are synced.
    virtual void syncDirectoriesToDisk() {}

    virtual const ReadSettings & getReadSettings() const = 0;
    virtual const WriteSettings & getWriteSettings() const = 0;
    virtual size_t getWriteBufferSize() const = 0;

    /// Settings effectively used by this writer (e.g. S3 request settings). Empty if none.
    virtual std::map<String, String> getSerializedSettings() const { return {}; }
};

}
