#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PrefixPath.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

constexpr const char * FILES_HEADER = "files: ";

}

std::string serializePrefixPath(const std::string & logical_path, const DirectoryRemoteInfo & directory)
{
    if (!directory.has_explicit_file_list)
        return logical_path;

    /// Sorted to make the contents deterministic.
    std::vector<std::pair<std::string_view, const FileRemoteInfo *>> files;
    files.reserve(directory.files.size());
    for (const auto & [name, info] : directory.files)
        files.emplace_back(name, &info);
    std::ranges::sort(files, {}, &std::pair<std::string_view, const FileRemoteInfo *>::first);

    WriteBufferFromOwnString out;
    writeString(logical_path, out);
    writeChar('\n', out);
    writeString(FILES_HEADER, out);
    writeIntText(files.size(), out);
    writeChar('\n', out);

    for (const auto & [name, info] : files)
    {
        writeEscapedString(name, out);
        writeChar('\t', out);
        writeEscapedString(getBlobKey(directory, std::string(name), *info), out);
        writeChar('\t', out);
        writeIntText(info->bytes_size, out);
        writeChar('\n', out);
    }

    return out.str();
}

PrefixPathContents parsePrefixPath(std::string_view contents)
{
    PrefixPathContents result;
    ReadBufferFromString in(contents);

    readStringUntilNewlineInto(result.logical_path, in);
    if (in.eof())
        return result;

    assertChar('\n', in);
    result.has_explicit_file_list = true;

    assertString(FILES_HEADER, in);
    size_t files_count = 0;
    readIntText(files_count, in);
    assertChar('\n', in);

    result.files.reserve(files_count);
    for (size_t i = 0; i < files_count; ++i)
    {
        PrefixPathContents::File file;
        readEscapedString(file.name, in);
        assertChar('\t', in);
        readEscapedString(file.blob_key, in);
        assertChar('\t', in);
        readIntText(file.bytes_size, in);
        assertChar('\n', in);

        if (file.name.empty() || file.blob_key.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Empty file name or blob key in the file list of the directory '{}'", result.logical_path);

        result.files.push_back(std::move(file));
    }

    if (!in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the file list of the directory '{}'", result.logical_path);

    return result;
}

}
