#include <Backups/BackupIO_Default.h>

#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Poco/Exception.h>

#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

/// OSX does not have O_DIRECTORY
#ifndef O_DIRECTORY
#define O_DIRECTORY O_RDONLY
#endif

namespace ProfileEvents
{
    extern const Event FileSync;
    extern const Event FileSyncElapsedMicroseconds;
    extern const Event DirectorySync;
    extern const Event DirectorySyncElapsedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_CLOSE_FILE;
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
}

std::unique_ptr<WriteBuffer> IBackupWriter::writeFileIfNotExists(const String & file_name)
{
    return writeFile(file_name);
}

void fsyncBackupFileContents(const fs::path & path)
{
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (-1 == fd)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {} for fsync", path.string());

    ProfileEvents::increment(ProfileEvents::FileSync);
    Stopwatch watch;
    try
    {
#if defined(OS_DARWIN)
        if (-1 == ::fsync(fd))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path, "Cannot fsync {}", path.string());
#else
        if (-1 == ::fdatasync(fd))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path, "Cannot fdatasync {}", path.string());
#endif
    }
    catch (...)
    {
        [[maybe_unused]] int err = ::close(fd);
        throw;
    }
    ProfileEvents::increment(ProfileEvents::FileSyncElapsedMicroseconds, watch.elapsedMicroseconds());

    if (-1 == ::close(fd))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_CLOSE_FILE, path, "Cannot close file {} after fsync", path.string());
}

void fsyncBackupDirectory(const fs::path & path)
{
    int fd = ::open(path.c_str(), O_DIRECTORY | O_CLOEXEC);
    if (-1 == fd)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open directory {} for fsync", path.string());

    ProfileEvents::increment(ProfileEvents::DirectorySync);
    Stopwatch watch;
    try
    {
#if defined(OS_DARWIN)
        if (-1 == ::fsync(fd))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path, "Cannot fsync directory {}", path.string());
#else
        if (-1 == ::fdatasync(fd))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path, "Cannot fdatasync directory {}", path.string());
#endif
    }
    catch (...)
    {
        [[maybe_unused]] int err = ::close(fd);
        throw;
    }
    ProfileEvents::increment(ProfileEvents::DirectorySyncElapsedMicroseconds, watch.elapsedMicroseconds());

    if (-1 == ::close(fd))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_CLOSE_FILE, path, "Cannot close directory {} after fsync", path.string());
}


BackupReaderDefault::BackupReaderDefault(const ReadSettings & read_settings_, const WriteSettings & write_settings_, LoggerPtr log_)
    : log(log_)
    , read_settings(read_settings_)
    , write_settings(write_settings_)
    , write_buffer_size(DBMS_DEFAULT_BUFFER_SIZE)
{
}

void BackupReaderDefault::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                         DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    LOG_TRACE(log, "Copying file {} to disk {} through buffers", path_in_backup, destination_disk->getName());

    auto read_buffer = readFile(path_in_backup);

    std::unique_ptr<WriteBuffer> write_buffer;
    auto buf_size = std::min(file_size, write_buffer_size);
    if (encrypted_in_backup)
        write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, write_settings);
    else
        write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, write_settings);

    copyData(*read_buffer, *write_buffer, file_size);
    write_buffer->finalize();
}

void BackupReaderDefault::copyFileRangeToDisk(const String & path_in_backup, size_t offset, size_t size, size_t /* file_size */,
                                              bool encrypted_in_backup, DiskPtr destination_disk, const String & destination_path,
                                              WriteMode write_mode)
{
    LOG_TRACE(log, "Copying a range of file {} to disk {} through buffers", path_in_backup, destination_disk->getName());

    auto read_buffer = readFile(path_in_backup);
    read_buffer->seek(offset, SEEK_SET);

    std::unique_ptr<WriteBuffer> write_buffer;
    auto buf_size = std::min(size, write_buffer_size);
    if (encrypted_in_backup)
        write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, write_settings);
    else
        write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, write_settings);

    copyData(*read_buffer, *write_buffer, size);
    write_buffer->finalize();
}

BackupWriterDefault::BackupWriterDefault(const ReadSettings & read_settings_, const WriteSettings & write_settings_, LoggerPtr log_)
    : log(log_)
    , read_settings(read_settings_)
    , write_settings(write_settings_)
    , write_buffer_size(DBMS_DEFAULT_BUFFER_SIZE)
{
}

bool BackupWriterDefault::fileContentsEqual(const String & file_name, const String & expected_file_contents, String & actual_file_contents)
try
{
    if (!fileExists(file_name))
        return false;

    auto in = readFile(file_name, expected_file_contents.size());
    actual_file_contents = String(expected_file_contents.size(), ' ');
    return (in->read(actual_file_contents.data(), actual_file_contents.size()) == actual_file_contents.size())
        && (actual_file_contents == expected_file_contents) && in->eof();
}
catch (const Exception &)
{
    throw;
}
catch (const Poco::Exception & ex)
{
    throw Exception(
        ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
        "Failed to check file {} contents: {}", file_name, ex.message());
}
catch (const std::exception & ex)
{
    throw Exception(
        ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
        "Failed to check file {} contents: {}", file_name, ex.what());
}

void BackupWriterDefault::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    auto read_buffer = create_read_buffer();

    if (start_pos)
        read_buffer->seek(start_pos, SEEK_SET);

    auto write_buffer = writeFile(path_in_backup);

    copyData(*read_buffer, *write_buffer, length);
    write_buffer->finalize();
}

void BackupWriterDefault::copyFileFromDisk(
    const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// Copy through buffers (derived classes may override with optimized implementations)
    LOG_TRACE(log, "Copying file {} from disk {} through buffers", src_path, src_disk->getName());

    auto create_read_buffer = [src_disk, src_path, copy_encrypted, settings = read_settings.adjustBufferSize(start_pos + length)]
    {
        if (copy_encrypted)
            return src_disk->readEncryptedFile(src_path, settings);
        return src_disk->readFile(src_path, settings);
    };

    copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
}

void BackupWriterDefault::removeFiles(const Strings & file_names)
{
    /// Derived classes can override removeFiles() to remove files faster (e.g. by using batch remove).
    for (const auto & file_name : file_names)
        removeFile(file_name);
}

void BackupWriterDefault::removeEmptyDirectories()
{
}

}
