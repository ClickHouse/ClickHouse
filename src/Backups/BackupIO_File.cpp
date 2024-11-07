#include <Backups/BackupIO_File.h>
#include <Disks/DiskLocal.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/logger_useful.h>


namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BackupReaderFile::BackupReaderFile(const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderFile"))
    , root_path(root_path_)
    , data_source_description(DiskLocal::getLocalDataSourceDescription(root_path))
{
}

bool BackupReaderFile::fileExists(const String & file_name)
{
    return fs::exists(root_path / file_name);
}

UInt64 BackupReaderFile::getFileSize(const String & file_name)
{
    return fs::file_size(root_path / file_name);
}

std::unique_ptr<SeekableReadBuffer> BackupReaderFile::readFile(const String & file_name)
{
    return createReadBufferFromFileBase(root_path / file_name, read_settings);
}

void BackupReaderFile::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                      DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    /// std::filesystem::copy() can copy from the filesystem only, and can't do throttling or appending.
    bool has_throttling = static_cast<bool>(read_settings.local_throttler);
    if (!has_throttling && (write_mode == WriteMode::Rewrite))
    {
        auto destination_data_source_description = destination_disk->getDataSourceDescription();
        if (destination_data_source_description.sameKind(data_source_description)
            && (destination_data_source_description.is_encrypted == encrypted_in_backup))
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} to disk {} locally", path_in_backup, destination_disk->getName());

            auto write_blob_function = [abs_source_path = root_path / path_in_backup, file_size](
                                           const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> &) -> size_t
            {
                /// For local disks the size of a blob path is expected to be 1.
                if (blob_path.size() != 1 || mode != WriteMode::Rewrite)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Blob writing function called with unexpected blob_path.size={} or mode={}",
                                    blob_path.size(), mode);
                fs::copy(abs_source_path, blob_path.at(0), fs::copy_options::overwrite_existing);
                return file_size;
            };

            destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


BackupWriterFile::BackupWriterFile(const String & root_path_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterFile"))
    , root_path(root_path_)
    , data_source_description(DiskLocal::getLocalDataSourceDescription(root_path))
{
}

bool BackupWriterFile::fileExists(const String & file_name)
{
    return fs::exists(root_path / file_name);
}

UInt64 BackupWriterFile::getFileSize(const String & file_name)
{
    return fs::file_size(root_path / file_name);
}

std::unique_ptr<ReadBuffer> BackupWriterFile::readFile(const String & file_name, size_t expected_file_size)
{
    return createReadBufferFromFileBase(root_path / file_name, read_settings.adjustBufferSize(expected_file_size));
}

std::unique_ptr<WriteBuffer> BackupWriterFile::writeFile(const String & file_name)
{
    auto file_path = root_path / file_name;
    fs::create_directories(file_path.parent_path());
    return std::make_unique<WriteBufferFromFile>(file_path, write_buffer_size, -1, write_settings.local_throttler);
}

void BackupWriterFile::removeFile(const String & file_name)
{
    (void)fs::remove(root_path / file_name);
}

void BackupWriterFile::removeFiles(const Strings & file_names)
{
    for (const auto & file_name : file_names)
        (void)fs::remove(root_path / file_name);
}

void BackupWriterFile::removeEmptyDirectories()
{
    removeEmptyDirectoriesImpl(root_path);
}

void BackupWriterFile::removeEmptyDirectoriesImpl(const fs::path & current_dir)
{
    if (!fs::is_directory(current_dir))
        return;

    if (fs::is_empty(current_dir))
    {
        (void)fs::remove(current_dir);
        return;
    }

    /// Backups are not too deep, so recursion is good enough here.
    for (const auto & it : std::filesystem::directory_iterator{current_dir})
        removeEmptyDirectoriesImpl(it.path());

    if (fs::is_empty(current_dir))
        (void)fs::remove(current_dir);
}

void BackupWriterFile::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                        bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// std::filesystem::copy() can copy from the filesystem only, and can't do throttling or copy a part of the file.
    bool has_throttling = static_cast<bool>(read_settings.local_throttler);
    if (!has_throttling)
    {
        auto source_data_source_description = src_disk->getDataSourceDescription();
        if (source_data_source_description.sameKind(data_source_description)
            && (source_data_source_description.is_encrypted == copy_encrypted))
        {
            /// std::filesystem::copy() can copy from a single file only.
            if (auto blob_path = src_disk->getBlobPath(src_path); blob_path.size() == 1)
            {
                const auto & abs_source_path = blob_path[0];

                /// std::filesystem::copy() can copy a file as a whole only.
                if ((start_pos == 0) && (length == fs::file_size(abs_source_path)))
                {
                    /// Use more optimal way.
                    LOG_TRACE(log, "Copying file {} from disk {} locally", src_path, src_disk->getName());
                    auto abs_dest_path = root_path / path_in_backup;
                    fs::create_directories(abs_dest_path.parent_path());
                    fs::copy(abs_source_path, abs_dest_path, fs::copy_options::overwrite_existing);
                    return; /// copied!
                }
            }
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterFile::copyFile(const String & destination, const String & source, size_t /*size*/)
{
    LOG_TRACE(log, "Copying file inside backup from {} to {} ", source, destination);

    auto abs_source_path = root_path / source;
    auto abs_dest_path = root_path / destination;
    fs::create_directories(abs_dest_path.parent_path());
    fs::copy(abs_source_path, abs_dest_path, fs::copy_options::overwrite_existing);
}

}
