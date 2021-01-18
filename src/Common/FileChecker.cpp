#include <common/JSON.h>
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
}


FileChecker::FileChecker(DiskPtr disk_, const String & file_info_path_) : disk(std::move(disk_))
{
    setPath(file_info_path_);
}

void FileChecker::setPath(const String & file_info_path_)
{
    files_info_path = file_info_path_;

    tmp_files_info_path = parentPath(files_info_path) + "tmp_" + fileName(files_info_path);
}

void FileChecker::update(const String & full_file_path)
{
    initialize();
    map[fileName(full_file_path)] = disk->getFileSize(full_file_path);
}

void FileChecker::setEmpty(const String & full_file_path)
{
    map[fileName(full_file_path)] = 0;
}

CheckResults FileChecker::check() const
{
    // Read the files again every time you call `check` - so as not to violate the constancy.
    // `check` method is rarely called.

    CheckResults results;
    Map local_map;
    load(local_map, files_info_path);

    if (local_map.empty())
        return {};

    for (const auto & name_size : local_map)
    {
        const String & name = name_size.first;
        String path = parentPath(files_info_path) + name;
        if (!disk->exists(path))
        {
            results.emplace_back(name, false, "File " + path + " doesn't exist");
            break;
        }

        auto real_size = disk->getFileSize(path);
        if (real_size != name_size.second)
        {
            results.emplace_back(name, false, "Size of " + path + " is wrong. Size is " + toString(real_size) + " but should be " + toString(name_size.second));
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
            LOG_WARNING(&Poco::Logger::get("FileChecker"), "Will truncate file {} that has size {} to size {}", path, real_size, expected_size);
            disk->truncateFile(path, expected_size);
        }
    }
}

void FileChecker::initialize()
{
    if (initialized)
        return;

    load(map, files_info_path);
    initialized = true;
}

void FileChecker::save() const
{
    {
        std::unique_ptr<WriteBuffer> out = disk->writeFile(tmp_files_info_path);

        /// So complex JSON structure - for compatibility with the old format.
        writeCString("{\"yandex\":{", *out);

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

void FileChecker::load(Map & local_map, const String & path) const
{
    local_map.clear();

    if (!disk->exists(path))
        return;

    std::unique_ptr<ReadBuffer> in = disk->readFile(path);
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

    JSON files = json["yandex"];
    for (const JSON file : files) // NOLINT
        local_map[unescapeForFileName(file.getName())] = file.getValue()["size"].toUInt();
}

}
