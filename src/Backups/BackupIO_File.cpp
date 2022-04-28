#include <Backups/BackupIO_File.h>
#include <Common/Exception.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <base/logger_useful.h>

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

size_t BackupReaderFile::getFileSize(const String & file_name)
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

std::unique_ptr<WriteBuffer> BackupWriterFile::writeFile(const String & file_name)
{
    auto file_path = path / file_name;
    fs::create_directories(file_path.parent_path());
    return std::make_unique<WriteBufferFromFile>(file_path);
}

void BackupWriterFile::removeFilesAfterFailure(const Strings & file_names)
{
    try
    {
        for (const auto & file_name : file_names)
            fs::remove(path / file_name);
        if (fs::is_directory(path) && fs::is_empty(path))
            fs::remove(path);
    }
    catch (...)
    {
        LOG_WARNING(&Poco::Logger::get("BackupWriterFile"), "RemoveFilesAfterFailure: {}", getCurrentExceptionMessage(false));
    }
}

}
