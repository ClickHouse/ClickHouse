#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PlainRewritableSnapshotFile.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

void writePlainRewritableSnapshot(const PlainRewritableRemoteLayout & layout, WriteBuffer & out)
{
    writeVarUInt(SNAPSHOT_FORMAT_VERSION, out);

    CompressedWriteBuffer compressed(out, CompressionCodecFactory::instance().get("ZSTD", {}));

    try
    {
        /// Ordered output makes the file deterministic and compresses better.
        std::vector<const PlainRewritableRemoteLayout::value_type *> directories;
        directories.reserve(layout.size());
        for (const auto & entry : layout)
            directories.push_back(&entry);
        std::ranges::sort(directories, {}, [](const auto * entry) -> const std::string & { return entry->first; });

        writeVarUInt(directories.size(), compressed);
        for (const auto * directory : directories)
        {
            const auto & [path, info] = *directory;
            writeStringBinary(path, compressed);
            writeStringBinary(info.remote_path, compressed);
            writeStringBinary(info.etag, compressed);
            writeIntBinary(static_cast<Int64>(info.last_modified), compressed);

            std::vector<const std::pair<const std::string, FileRemoteInfo> *> files;
            files.reserve(info.files.size());
            for (const auto & entry : info.files)
                files.push_back(&entry);
            std::ranges::sort(files, {}, [](const auto * entry) -> const std::string & { return entry->first; });

            writeVarUInt(files.size(), compressed);
            for (const auto * file : files)
            {
                const auto & [name, file_info] = *file;
                writeStringBinary(name, compressed);
                writeVarUInt(file_info.bytes_size, compressed);
                writeIntBinary(static_cast<Int64>(file_info.last_modified), compressed);
            }
        }

        compressed.finalize();
    }
    catch (...)
    {
        compressed.cancel();
        throw;
    }
}

PlainRewritableRemoteLayout readPlainRewritableSnapshot(ReadBuffer & in)
{
    UInt64 version = 0;
    readVarUInt(version, in);
    if (version != SNAPSHOT_FORMAT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown version {} of the plain_rewritable snapshot file, expected {}", version, SNAPSHOT_FORMAT_VERSION);

    CompressedReadBuffer compressed(in);

    PlainRewritableRemoteLayout layout;

    UInt64 directories_count = 0;
    readVarUInt(directories_count, compressed);
    layout.reserve(directories_count);

    for (UInt64 i = 0; i < directories_count; ++i)
    {
        std::string path;
        readStringBinary(path, compressed);

        DirectoryRemoteInfo info;
        readStringBinary(info.remote_path, compressed);
        readStringBinary(info.etag, compressed);
        Int64 last_modified = 0;
        readIntBinary(last_modified, compressed);
        info.last_modified = static_cast<time_t>(last_modified);

        UInt64 files_count = 0;
        readVarUInt(files_count, compressed);
        info.files.reserve(files_count);
        for (UInt64 j = 0; j < files_count; ++j)
        {
            std::string name;
            readStringBinary(name, compressed);

            FileRemoteInfo file_info;
            readVarUInt(file_info.bytes_size, compressed);
            Int64 file_last_modified = 0;
            readIntBinary(file_last_modified, compressed);
            file_info.last_modified = static_cast<time_t>(file_last_modified);

            if (!info.files.emplace(std::move(name), file_info).second)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate file in the directory '{}' of the plain_rewritable snapshot file", path);
        }

        if (!layout.emplace(std::move(path), std::move(info)).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate directory in the plain_rewritable snapshot file");
    }

    if (!compressed.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the end of the plain_rewritable snapshot file");

    return layout;
}

}
