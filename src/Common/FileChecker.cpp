#include <base/JSON.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/escapeForFileName.h>

#include <Common/FileChecker.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int LOGICAL_ERROR;
}


FileChecker::FileChecker(DiskPtr disk_, const String & file_info_path_) : disk(std::move(disk_))
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
    bool exists = disk->exists(full_file_path);
    auto real_size = exists ? disk->getFileSize(full_file_path) : 0;  /// No race condition assuming no one else is working with these files.
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

CheckResults FileChecker::check() const
{
    if (map.empty())
        return {};

    CheckResults results;

    for (const auto & name_size : map)
    {
        const String & name = name_size.first;
        String path = parentPath(files_info_path) + name;
        bool exists = disk->exists(path);
        auto real_size = exists ? disk->getFileSize(path) : 0;  /// No race condition assuming no one else is working with these files.

        if (real_size != name_size.second)
        {
            String failure_message = exists
                ? ("Size of " + path + " is wrong. Size is " + toString(real_size) + " but should be " + toString(name_size.second))
                : ("File " + path + " doesn't exist");
            results.emplace_back(name, false, failure_message);
            break;
        }

        results.emplace_back(name, true, "");
    }

    return results;
}

void FileChecker::repair()
{
    for (const auto & name_size : map)
    {
        const String & name = name_size.first;
        size_t expected_size = name_size.second;
        String path = parentPath(files_info_path) + name;
        bool exists = disk->exists(path);
        auto real_size = exists ? disk->getFileSize(path) : 0;  /// No race condition assuming no one else is working with these files.

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
        std::unique_ptr<WriteBuffer> out = disk->writeFile(tmp_files_info_path);

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
        out->next();
    }

    disk->replaceFile(tmp_files_info_path, files_info_path);
}

void FileChecker::load()
{
    map.clear();

    if (!disk->exists(files_info_path))
        return;

    std::unique_ptr<ReadBuffer> in = disk->readFile(files_info_path);
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

}
