#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ErrorCodes.h>
#include <Disks/IDisk.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <base/JSON.h>

#include <boost/range/adaptor/map.hpp>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int LOGICAL_ERROR;
}


FileChecker::FileChecker(const String & file_info_path_) : FileChecker(nullptr, file_info_path_)
{
}

FileChecker::FileChecker(DiskPtr disk_, const String & file_info_path_)
    : disk(std::move(disk_))
    , log(getLogger("FileChecker"))
{
    setPath(file_info_path_);
    try
    {
        load();
    }
    catch (DB::Exception & e)
    {
        e.addMessage("Error loading file {}", files_info_path);
        throw;
    }
}

void FileChecker::setPath(const String & file_info_path_)
{
    files_info_path = file_info_path_;
}

String FileChecker::getPath() const
{
    return files_info_path;
}

void FileChecker::update(const String & full_file_path)
{
    bool exists = fileReallyExists(full_file_path);
    auto real_size = exists ? getRealFileSize(full_file_path) : 0;  /// No race condition assuming no one else is working with these files.
    map[fileName(full_file_path)] = real_size;
}

void FileChecker::setEmpty(const String & full_file_path)
{
    map[fileName(full_file_path)] = 0;
}

size_t FileChecker::getFileSize(const String & full_file_path) const
{
    auto it = map.find(fileName(full_file_path));
    if (it == map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} is not added to the file checker", full_file_path);
    return it->second;
}

size_t FileChecker::getTotalSize() const
{
    size_t total_size = 0;
    for (auto size : map | boost::adaptors::map_values)
        total_size += size;
    return total_size;
}


FileChecker::DataValidationTasksPtr FileChecker::getDataValidationTasks()
{
    return std::make_unique<DataValidationTasks>(map);
}

std::optional<CheckResult> FileChecker::checkNextEntry(DataValidationTasksPtr & check_data_tasks) const
{
    String name;
    size_t expected_size;
    bool is_finished = check_data_tasks->next(name, expected_size);
    if (is_finished)
        return {};

    String path = parentPath(files_info_path) + name;
    bool exists = fileReallyExists(path);
    auto real_size = exists ? getRealFileSize(path) : 0;  /// No race condition assuming no one else is working with these files.

    if (real_size != expected_size)
    {
        String failure_message = exists
            ? ("Size of " + path + " is wrong. Size is " + toString(real_size) + " but should be " + toString(expected_size))
            : ("File " + path + " doesn't exist");
        return CheckResult(name, false, failure_message);
    }

    return CheckResult(name, true, "");
}

void FileChecker::repair()
{
    for (const auto & name_size : map)
    {
        const String & name = name_size.first;
        size_t expected_size = name_size.second;
        String path = parentPath(files_info_path) + name;
        bool exists = fileReallyExists(path);
        auto real_size = exists ? getRealFileSize(path) : 0;  /// No race condition assuming no one else is working with these files.

        if (real_size < expected_size)
            throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "Size of {} is less than expected. Size is {} but should be {}.",
                path, real_size, expected_size);

        if (real_size > expected_size)
        {
            LOG_WARNING(log, "Will truncate file {} that has size {} to size {}", path, real_size, expected_size);
            disk->truncateFile(path, expected_size);
        }
    }
}

void FileChecker::save() const
{
    std::string tmp_files_info_path = parentPath(files_info_path) + "tmp_" + fileName(files_info_path);

    {
        std::unique_ptr<WriteBufferFromFileBase> out = disk ? disk->writeFile(tmp_files_info_path) : std::make_unique<WriteBufferFromFile>(tmp_files_info_path);

        /// So complex JSON structure - for compatibility with the old format.
        writeCString("{\"clickhouse\":{", *out);

        auto settings = FormatSettings();
        for (auto it = map.begin(); it != map.end(); ++it)
        {
            if (it != map.begin())
                writeString(",", *out);

            /// `escapeForFileName` is not really needed. But it is left for compatibility with the old code.
            writeJSONString(escapeForFileName(it->first), *out, settings);
            writeString(R"(:{"size":")", *out);
            writeIntText(it->second, *out);
            writeString("\"}", *out);
        }

        writeCString("}}", *out);

        out->sync();
        out->finalize();
    }

    if (disk)
        disk->replaceFile(tmp_files_info_path, files_info_path);
    else
        fs::rename(tmp_files_info_path, files_info_path);
}

void FileChecker::load()
{
    map.clear();

    if (!fileReallyExists(files_info_path))
        return;

    std::unique_ptr<ReadBuffer> in = disk ? disk->readFile(files_info_path, getReadSettings()) : std::make_unique<ReadBufferFromFile>(files_info_path);
    WriteBufferFromOwnString out;

    /// The JSON library does not support whitespace. We delete them. Inefficient.
    while (!in->eof())
    {
        char c;
        readChar(c, *in);
        if (!isspace(c))
            writeChar(c, out);
    }
    JSON json(out.str());

    JSON files = json.has("clickhouse") ? json["clickhouse"] : json["yandex"];
    for (const JSON file : files) // NOLINT
        map[unescapeForFileName(file.getName())] = file.getValue()["size"].toUInt();
}

bool FileChecker::fileReallyExists(const String & path_) const
{
    return disk ? disk->existsFile(path_) : fs::exists(path_);
}

size_t FileChecker::getRealFileSize(const String & path_) const
{
    return disk ? disk->getFileSize(path_) : fs::file_size(path_);
}

}
