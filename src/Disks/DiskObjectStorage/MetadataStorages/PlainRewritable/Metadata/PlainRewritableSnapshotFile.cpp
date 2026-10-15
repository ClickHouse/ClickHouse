#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PlainRewritableSnapshotFile.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <base/unaligned.h>
#include <city.h>

#include <algorithm>
#include <unordered_set>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{

/// The strings of the snapshot are paths, object keys and ETags, which are far shorter than this. The lengths are not
/// trusted: the string grows as its bytes are actually read, so a corrupted length fails the parsing at the end of the
/// file instead of allocating up to this much memory upfront.
constexpr size_t MAX_SNAPSHOT_STRING_SIZE = 64 * 1024;

void readSnapshotString(std::string & s, ReadBuffer & in)
{
    readStringBinaryGrowing(s, in, MAX_SNAPSHOT_STRING_SIZE);
}

/// The size of the uncompressed blocks the snapshot is written in.
constexpr size_t SNAPSHOT_BLOCK_SIZE = 1024 * 1024;

/// `CompressedReadBuffer` takes the sizes of a block from its header and allocates the memory for the compressed block and
/// for the decompressed data before it reads the block and verifies its checksum, up to 1 GiB each. The header is not
/// trusted: every block must be written by `writePlainRewritableSnapshot` with `ZSTD`, be completely present in `data`,
/// and decompress to at most `SNAPSHOT_BLOCK_SIZE` bytes. Then a block is decompressed right from `data`, and a corrupted
/// snapshot file fails to parse rather than allocate a huge amount of memory.
void validateSnapshotBlocks(std::string_view data)
{
    constexpr size_t checksum_size = sizeof(CityHash_v1_0_2::uint128);
    constexpr size_t header_size = COMPRESSED_BLOCK_HEADER_SIZE;

    while (!data.empty())
    {
        if (data.size() < checksum_size + header_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Truncated header of a compressed block in the plain_rewritable snapshot file");

        const char * header = data.data() + checksum_size;
        const UInt8 method = ICompressionCodec::readMethod(header);
        /// Both sizes include the header.
        const size_t compressed_size = unalignedLoadLittleEndian<UInt32>(header + 1);
        const size_t decompressed_size = unalignedLoadLittleEndian<UInt32>(header + 5);

        if (method != static_cast<UInt8>(CompressionMethodByte::ZSTD))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected compression method 0x{:02x} of a block in the plain_rewritable snapshot file", static_cast<UInt32>(method));
        if (compressed_size < header_size || compressed_size > data.size() - checksum_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid size {} of a compressed block in the plain_rewritable snapshot file", compressed_size);
        if (decompressed_size == 0 || decompressed_size > SNAPSHOT_BLOCK_SIZE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid decompressed size {} of a block in the plain_rewritable snapshot file", decompressed_size);

        data.remove_prefix(checksum_size + compressed_size);
    }
}

}

void writePlainRewritableSnapshot(const PlainRewritableRemoteLayout & layout, WriteBuffer & out)
{
    writeVarUInt(SNAPSHOT_FORMAT_VERSION, out);

    CompressedWriteBuffer compressed(out, CompressionCodecFactory::instance().get("ZSTD", {}), SNAPSHOT_BLOCK_SIZE);

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
            writeBinary(static_cast<UInt8>(info.has_explicit_file_list), compressed);

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
                writeStringBinary(file_info.blob_key, compressed);
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

    /// The data grows as its bytes are actually read, so the memory is proportional to the size of the file.
    std::string compressed_data;
    readStringUntilEOF(compressed_data, in);
    validateSnapshotBlocks(compressed_data);

    ReadBufferFromString compressed_in(compressed_data);
    CompressedReadBuffer compressed(compressed_in);

    PlainRewritableRemoteLayout layout;

    UInt64 directories_count = 0;
    readVarUInt(directories_count, compressed);

    /// The counts are not trusted, so the containers are not reserved from them:
    /// a corrupted snapshot file must fail to parse rather than allocate a huge amount of memory.

    /// Every directory has its own remote path. The loader matches the directories of the listing to the snapshot
    /// by the remote path, so a duplicate would let it load a directory under the logical path of another one.
    std::unordered_set<std::string> remote_paths;

    for (UInt64 i = 0; i < directories_count; ++i)
    {
        std::string path;
        readSnapshotString(path, compressed);

        DirectoryRemoteInfo info;
        readSnapshotString(info.remote_path, compressed);
        readSnapshotString(info.etag, compressed);
        Int64 last_modified = 0;
        readIntBinary(last_modified, compressed);
        info.last_modified = static_cast<time_t>(last_modified);
        UInt8 has_explicit_file_list = 0;
        readBinary(has_explicit_file_list, compressed);
        if (has_explicit_file_list > 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid flag of the directory '{}' in the plain_rewritable snapshot file", path);
        info.has_explicit_file_list = has_explicit_file_list;

        if (!remote_paths.insert(info.remote_path).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate remote path of the directory '{}' in the plain_rewritable snapshot file", path);

        UInt64 files_count = 0;
        readVarUInt(files_count, compressed);
        for (UInt64 j = 0; j < files_count; ++j)
        {
            std::string name;
            readSnapshotString(name, compressed);

            FileRemoteInfo file_info{};
            readVarUInt(file_info.bytes_size, compressed);
            Int64 file_last_modified = 0;
            readIntBinary(file_last_modified, compressed);
            file_info.last_modified = static_cast<time_t>(file_last_modified);
            readSnapshotString(file_info.blob_key, compressed);

            if (!info.files.emplace(std::move(name), file_info).second)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate file in the directory '{}' of the plain_rewritable snapshot file", path);
        }

        if (!layout.emplace(std::move(path), std::move(info)).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate directory in the plain_rewritable snapshot file");
    }

    if (!compressed.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the end of the plain_rewritable snapshot file");

    /// The state of a disk always has the root directory, and the snapshot is applied as is.
    if (!layout.contains(""))
        throw Exception(ErrorCodes::INCORRECT_DATA, "There is no root directory in the plain_rewritable snapshot file");

    return layout;
}

}
