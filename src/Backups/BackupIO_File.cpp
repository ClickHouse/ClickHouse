#include <Backups/BackupIO_File.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>

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

void BackupWriterFile::removeFiles(const Strings & file_names)
{
    for (const auto & file_name : file_names)
        fs::remove(path / file_name);
    if (fs::is_directory(path) && fs::is_empty(path))
        fs::remove(path);
}

}
