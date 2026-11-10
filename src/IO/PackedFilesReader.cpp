#include <Disks/IDisk.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileView.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int FILE_DOESNT_EXIST;
    extern const int CORRUPTED_DATA;
}

PackedFilesReader::PackedFilesReader(
    DiskPtr disk_, const String & data_file_name_, const ReadSettings & read_settings_)
    : disk(std::move(disk_)), data_file_name(data_file_name_)
{
    auto in = disk->readFile(data_file_name, read_settings_.adjustBufferSize(4096));

    index = readIndex(*in);
}

PackedFilesReader::PackedFilesReader(
    DiskPtr disk_, const String & data_file_name_, const PackedFilesIO::Index & index_)
    : disk(std::move(disk_)), data_file_name(data_file_name_), index(index_)
{
}


PackedFilesIO::Index PackedFilesReader::readIndex(ReadBuffer & in)
{
    UInt8 version;
    readIntBinary(version, in);

    if (version > PackedFilesIO::VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unknown format ({}) of packed data", std::to_string(version));

    PackedFilesIO::Index index;
    size_t num_files;
    readIntBinary(num_files, in);

    for (size_t i = 0; i < num_files; ++i)
    {
        String file_name;
        PackedFilesIO::FileOffset offset;

        readStringBinary(file_name, in);
        readIntBinary(offset.offset, in);
        readIntBinary(offset.size, in);

        if (!index.emplace(file_name, offset).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicated file: {}", file_name);
    }
    return index;
}

bool PackedFilesReader::exists(const std::string & file_name) const
{
    return index.contains(file_name);
}

size_t PackedFilesReader::getFileSize(const String & file_name) const
{
    if (auto it = index.find(file_name); it != index.end())
        return it->second.size;

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", file_name);
}

Names PackedFilesReader::getFileNames() const
{
    Names res;
    for (const auto & [name, _] : index)
        res.emplace_back(name);
    return res;
}

static ReadSettings patchSettings(ReadSettings settings)
{
    settings.direct_io_threshold = 0;
    if (settings.local_fs_method == LocalFSReadMethod::mmap)
        settings.local_fs_method = LocalFSReadMethod::pread;
    return settings;
}

std::unique_ptr<ReadBufferFromFileBase> PackedFilesReader::readFile(
    const String & file_name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    auto it = index.find(file_name);
    if (it == index.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", file_name);

    const auto & [offset, size] = it->second;

    /// ReadBufferFromFileView doesn't support reading with mmap and direct io methods, because
    /// they require special alignment which cannot be achieved while reading archive file.
    /// So just disable them.
    auto in = disk->readFile(data_file_name, patchSettings(settings), read_hint);
    return std::make_unique<ReadBufferFromFileView>(std::move(in), file_name, offset, offset + size);
}

}
