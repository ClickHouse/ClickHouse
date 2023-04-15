#include <Backups/BackupIO_File.h>
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
    : IBackupReader(&Poco::Logger::get("BackupReaderFile")), path(path_)
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

void BackupReaderFile::copyFileToDisk(const String & file_name, size_t size, DiskPtr destination_disk, const String & destination_path,
                                      WriteMode write_mode, const WriteSettings & write_settings)
{
    if (destination_disk->getDataSourceDescription() == getDataSourceDescription())
    {
        /// Use more optimal way.
        LOG_TRACE(log, "Copying file {} locally", file_name);
        fs::copy(path / file_name, fullPath(destination_disk, destination_path), fs::copy_options::overwrite_existing);
        return;
    }

    /// Fallback to copy through buffers.
    IBackupReader::copyFileToDisk(path / file_name, size, destination_disk, destination_path, write_mode, write_settings);
}


BackupWriterFile::BackupWriterFile(const String & path_, const ContextPtr & context_)
    : IBackupWriter(context_, &Poco::Logger::get("BackupWriterFile"))
    , path(path_)
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

DataSourceDescription BackupWriterFile::getDataSourceDescription() const
{
    DataSourceDescription data_source_description;

    data_source_description.type = DataSourceType::Local;

    if (auto block_device_id = tryGetBlockDeviceId(path); block_device_id.has_value())
        data_source_description.description = *block_device_id;
    else
        data_source_description.description = path;
    data_source_description.is_encrypted = false;
    data_source_description.is_cached = false;

    return data_source_description;
}

DataSourceDescription BackupReaderFile::getDataSourceDescription() const
{
    DataSourceDescription data_source_description;

    data_source_description.type = DataSourceType::Local;

    if (auto block_device_id = tryGetBlockDeviceId(path); block_device_id.has_value())
        data_source_description.description = *block_device_id;
    else
        data_source_description.description = path;
    data_source_description.is_encrypted = false;
    data_source_description.is_cached = false;

    return data_source_description;
}


void BackupWriterFile::copyFileFromDisk(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name)
{
    /// std::filesystem::copy() can copy from the filesystem only, and it cannot do the throttling.
    if (!has_throttling && (getDataSourceDescription() == src_disk->getDataSourceDescription()))
    {
        std::string abs_source_path = fullPath(src_disk, src_file_name);
        /// std::filesystem::copy() can copy a file as a whole only.
        if ((src_offset == 0) && (src_size == fs::file_size(abs_source_path)))
        {
            /// Use more optimal way.
            LOG_TRACE(log, "Copying file {} locally", src_file_name);
            auto abs_dest_path = path / dest_file_name;
            fs::create_directories(abs_dest_path.parent_path());
            fs::copy(abs_source_path, abs_dest_path, fs::copy_options::overwrite_existing);
            return;
        }
    }

    /// Fallback to copy through buffers.
    IBackupWriter::copyFileFromDisk(src_disk, src_file_name, src_offset, src_size, dest_file_name);
}

}
