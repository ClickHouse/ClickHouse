#include <Backups/BackupIO_File.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>


namespace fs = std::filesystem;


namespace DB
{

BackupReaderFile::BackupReaderFile(const String & path_)
    : IBackupReader(&Poco::Logger::get("BackupReaderFile"))
    , path(path_)
    , data_source_description(DiskLocal::getLocalDataSourceDescription(path_))
{
}

BackupReaderFile::~BackupReaderFile() = default;

bool BackupReaderFile::fileExists(const String & file_name)
{
    return fs::exists(path / file_name);
}

UInt64 BackupReaderFile::getFileSize(const String & file_name)
{
    return fs::file_size(path / file_name);
}

std::unique_ptr<SeekableReadBuffer> BackupReaderFile::readFile(const String & file_name)
{
    return createReadBufferFromFileBase(path / file_name, {});
}

void BackupReaderFile::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                      DiskPtr destination_disk, const String & destination_path, WriteMode write_mode, const WriteSettings & write_settings)
{
    if (write_mode == WriteMode::Rewrite)
    {
        auto destination_data_source_description = destination_disk->getDataSourceDescription();
        if (destination_data_source_description.sameKind(data_source_description)
            && (destination_data_source_description.is_encrypted == encrypted_in_backup))
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} to disk {} locally", path_in_backup, destination_disk->getName());

            auto write_blob_function
                = [abs_source_path = path / path_in_backup, file_size](
                      const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> &) -> size_t
            {
                if (blob_path.size() != 1 || mode != WriteMode::Rewrite)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Blob writing function called with unexpected blob_path.size={} or mode={}",
                                    blob_path.size(), mode);
                fs::copy(abs_source_path, blob_path.at(0), fs::copy_options::overwrite_existing);
                return file_size;
            };

            destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
            return;
        }
    }

    /// Fallback to copy through buffers.
    IBackupReader::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode, write_settings);
}


BackupWriterFile::BackupWriterFile(const String & path_, const ContextPtr & context_)
    : IBackupWriter(context_, &Poco::Logger::get("BackupWriterFile"))
    , path(path_)
    , data_source_description(DiskLocal::getLocalDataSourceDescription(path_))
    , has_throttling(static_cast<bool>(context_->getBackupsThrottler()))
{
}

BackupWriterFile::~BackupWriterFile() = default;

bool BackupWriterFile::fileExists(const String & file_name)
{
    return fs::exists(path / file_name);
}

UInt64 BackupWriterFile::getFileSize(const String & file_name)
{
    return fs::file_size(path / file_name);
}

bool BackupWriterFile::fileContentsEqual(const String & file_name, const String & expected_file_contents)
{
    if (!fs::exists(path / file_name))
        return false;

    try
    {
        auto in = createReadBufferFromFileBase(path / file_name, {});
        String actual_file_contents(expected_file_contents.size(), ' ');
        return (in->read(actual_file_contents.data(), actual_file_contents.size()) == actual_file_contents.size())
            && (actual_file_contents == expected_file_contents) && in->eof();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
}

std::unique_ptr<WriteBuffer> BackupWriterFile::writeFile(const String & file_name)
{
    auto file_path = path / file_name;
    fs::create_directories(file_path.parent_path());
    return std::make_unique<WriteBufferFromFile>(file_path);
}

void BackupWriterFile::removeFile(const String & file_name)
{
    fs::remove(path / file_name);
    if (fs::is_directory(path) && fs::is_empty(path))
        fs::remove(path);
}

void BackupWriterFile::removeFiles(const Strings & file_names)
{
    for (const auto & file_name : file_names)
        fs::remove(path / file_name);
    if (fs::is_directory(path) && fs::is_empty(path))
        fs::remove(path);
}

void BackupWriterFile::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                        bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// std::filesystem::copy() can copy from the filesystem only, and it cannot do the throttling.
    if (!has_throttling)
    {
        auto source_data_source_description = src_disk->getDataSourceDescription();
        if (source_data_source_description.sameKind(data_source_description)
            && (source_data_source_description.is_encrypted == copy_encrypted))
        {
            if (auto blob_path = src_disk->getBlobPath(src_path); blob_path.size() == 1)
            {
                auto abs_source_path = blob_path[0];

                /// std::filesystem::copy() can copy a file as a whole only.
                if ((start_pos == 0) && (length == fs::file_size(abs_source_path)))
                {
                    /// Use more optimal way.
                    LOG_TRACE(log, "Copying file {} from disk {} locally", src_path, src_disk->getName());
                    auto abs_dest_path = path / path_in_backup;
                    fs::create_directories(abs_dest_path.parent_path());
                    fs::copy(abs_source_path, abs_dest_path, fs::copy_options::overwrite_existing);
                    return;
                }
            }
        }
    }

    /// Fallback to copy through buffers.
    IBackupWriter::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

}
