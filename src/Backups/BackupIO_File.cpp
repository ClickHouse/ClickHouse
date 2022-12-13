#include <Backups/BackupIO_File.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/filesystemHelpers.h>

namespace fs = std::filesystem;


namespace DB
{
BackupReaderFile::BackupReaderFile(const String & path_) : path(path_)
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

BackupWriterFile::BackupWriterFile(const String & path_) : path(path_)
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


bool BackupWriterFile::supportNativeCopy(DataSourceDescription data_source_description) const
{
    return data_source_description == getDataSourceDescription();
}

void BackupWriterFile::copyFileNative(DiskPtr from_disk, const String & file_name_from, const String & file_name_to)
{
    auto file_path = path / file_name_to;
    fs::create_directories(file_path.parent_path());
    std::string abs_source_path;
    if (from_disk)
        abs_source_path = fullPath(from_disk, file_name_from);
    else
        abs_source_path = fs::absolute(file_name_from);

    fs::copy(abs_source_path, file_path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
}

}
